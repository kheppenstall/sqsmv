package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	var err error

	var live bool
	var limit int64
	var src, dest, dupdest, dedup, move, ignore, moveFile, ignoreFile string

	flag.BoolVar(&live, "live-run", false, "perform changes")
	flag.Int64Var(&limit, "limit", 0, "stop after n messages")
	flag.StringVar(&src, "src", "", "source queue")
	flag.StringVar(&dest, "dest", "", "destination queue")
	flag.StringVar(&dupdest, "dup-dest", "", "duplicate destination queue")
	flag.StringVar(&dedup, "dedup", "", "json field name")
	flag.StringVar(&move, "move", "", "move messages containing this substring")
	flag.StringVar(&moveFile, "move-file", "", "move messages containing at least one substring from this file")
	flag.StringVar(&ignore, "ignore", "", "do not move messages containing this substring")
	flag.StringVar(&ignoreFile, "ignore-file", "", "do not move messages containing any substring from this file")
	flag.Parse()

	if move != "" && moveFile != "" {
		log.Fatalln("--move and --move-file cannot both be specified")
	}

	if ignore != "" && ignoreFile != "" {
		log.Fatalln("--ignore and --ignore-file cannot both be specified")
	}

	if dedup != "" && dupdest == "" {
		log.Fatalln("--dup-dest required if --dedup specified")
	}

	if src == "" || (dest == "" && live) {
		flag.Usage()
		log.Fatalln("--dest must be specified when used with --live-run")
	}

	// always matches by default
	moveRegex := regexp.MustCompile(`^`)

	// never matches by default
	ignoreRegex := regexp.MustCompile(`x^`)

	if move != "" {
		moveRegex, err = buildRegexp(move)
		if err != nil {
			log.Fatalln("--move:", err)
		}
	} else if moveFile != "" {
		moveRegex, err = loadSubstrings(moveFile)
		if err != nil {
			log.Fatalln("--move-file:", err)
		}
	}

	if ignore != "" {
		ignoreRegex, err = buildRegexp(ignore)
		if err != nil {
			log.Fatalln("--ignore:", err)
		}
	} else if ignoreFile != "" {
		ignoreRegex, err = loadSubstrings(ignoreFile)
		if err != nil {
			log.Fatalln("--ignore-file:", err)
		}
	}

	srcRegion := urlRegion(src)
	destRegion := urlRegion(dest)
	dupDestRegion := urlRegion(dupdest)

	log.Printf("source queue [%s]: %v", srcRegion, src)
	log.Printf("destination queue [%s]: %v", destRegion, dest)
	log.Printf("duplicate destination queue [%s]: %v", dupDestRegion, dupdest)

	// enable automatic use of AWS_PROFILE like awscli and other tools do.
	opts := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}

	sess, err := session.NewSessionWithOptions(opts)
	if err != nil {
		panic(err)
	}

	var dupDestSess *session.Session
	var dupDestClient *sqs.SQS

	srcSess := sess.Copy(&aws.Config{Region: aws.String(srcRegion)})
	destSess := sess.Copy(&aws.Config{Region: aws.String(destRegion)})

	srcClient := sqs.New(srcSess)
	destClient := sqs.New(destSess)

	if dupdest != "" {
		dupDestSess = sess.Copy(&aws.Config{Region: aws.String(dupDestRegion)})
		dupDestClient = sqs.New(dupDestSess)
	}

	maxMessages := int64(10)
	waitTime := int64(0)
	messageAttributeNames := aws.StringSlice([]string{"All"})

	rmin := &sqs.ReceiveMessageInput{
		QueueUrl:              &src,
		MaxNumberOfMessages:   &maxMessages,
		WaitTimeSeconds:       &waitTime,
		MessageAttributeNames: messageAttributeNames,
		AttributeNames:        aws.StringSlice([]string{"MessageGroupId"}),
	}

	checkDup := deduper(dedup)

	lastMessageCount := int(1)
	// loop as long as there are messages on the queue
	i := limit
	for limit == 0 || i > 0 {
		if limit != 0 && *rmin.MaxNumberOfMessages > i {
			*rmin.MaxNumberOfMessages = i
		}
		i -= *rmin.MaxNumberOfMessages

		resp, err := srcClient.ReceiveMessage(rmin)

		if err != nil {
			panic(err)
		}

		if lastMessageCount == 0 && len(resp.Messages) == 0 {
			// no messages returned twice now, the queue is probably empty
			log.Printf("done")
			return
		}

		lastMessageCount = len(resp.Messages)
		log.Printf("received %v messages...", len(resp.Messages))

		var wg sync.WaitGroup

		for _, m := range resp.Messages {
			if ignoreRegex.MatchString(*m.Body) {
				log.Printf("ignoring %#q", *m.Body)
				continue
			}

			if !moveRegex.MatchString(*m.Body) {
				log.Printf("not moving %#q", *m.Body)
				continue
			}

			sendQueue := dest
			sendClient := destClient

			err = checkDup(*m.Body)
			if err == ErrDup {
				sendQueue = dupdest
				sendClient = dupDestClient
				err = nil
			}
			if err != nil {
				panic(err)
			}

			if !live {
				fmt.Println(*m.Body)
				continue
			}

			wg.Add(1)

			go func(m *sqs.Message) {
				defer wg.Done()

				// write the message to the destination queue
				smi := sqs.SendMessageInput{
					QueueUrl:          &sendQueue,
					MessageAttributes: m.MessageAttributes,
					MessageBody:       m.Body,
					MessageGroupId:    m.Attributes["MessageGroupId"],
				}

				_, err := sendClient.SendMessage(&smi)

				if err != nil {
					log.Printf("ERROR sending message to destination %v", err)
					return
				}

				// message was sent, dequeue from source queue
				dmi := &sqs.DeleteMessageInput{
					QueueUrl:      &src,
					ReceiptHandle: m.ReceiptHandle,
				}

				if _, err := srcClient.DeleteMessage(dmi); err != nil {
					log.Printf("ERROR dequeueing message ID %v : %v",
						*m.ReceiptHandle,
						err)
				}
			}(m)
		}

		// wait for all jobs from this batch...
		wg.Wait()
	}
}

func urlRegion(s string) string {
	if s == "" {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	s = u.Hostname()
	i := strings.IndexByte(s, '.')
	s = s[i+1:]
	i = strings.IndexByte(s, '.')
	return s[:i]
}

var ErrDup = errors.New("duplicate")

func deduper(field string) func(string) error {
	if field == "" {
		return func(string) error { return nil }
	}
	attrs := make(map[string]json.RawMessage)
	seen := make(map[string]struct{})

	return func(msg string) error {
		clearMap(attrs)
		err := json.Unmarshal([]byte(msg), &attrs)
		if err != nil {
			return err
		}
		v := attrs[field]
		if len(v) == 0 {
			return nil
		}
		_, ok := seen[string(v)]
		seen[string(v)] = struct{}{}
		if !ok {
			return nil
		}
		log.Printf("dup on %s", v)
		return ErrDup
	}
}

func clearMap(m map[string]json.RawMessage) {
	for k := range m {
		delete(m, k)
	}
}

func loadSubstrings(path string) (*regexp.Regexp, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var substrings []string

	sc := bufio.NewScanner(f)

	for sc.Scan() {
		substrings = append(substrings, sc.Text())
	}

	err = sc.Err()
	if err != nil {
		return nil, err
	}

	return buildRegexp(substrings...)
}

func buildRegexp(substrings ...string) (*regexp.Regexp, error) {
	for i, s := range substrings {
		substrings[i] = regexp.QuoteMeta(s)
	}

	pattern := strings.Join(substrings, "|")
	return regexp.Compile(pattern)
}

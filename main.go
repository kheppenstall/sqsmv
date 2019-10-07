package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	var live bool
	var limit int64
	var src, dest, dupdest, dedup, keep, drop string
	flag.BoolVar(&live, "live-run", false, "perform changes")
	flag.Int64Var(&limit, "limit", 0, "stop after n messages")
	flag.StringVar(&src, "src", "", "source queue")
	flag.StringVar(&dest, "dest", "", "destination queue")
	flag.StringVar(&dupdest, "dup-dest", "", "duplicate destination queue")
	flag.StringVar(&dedup, "dedup", "", "json field name")
	flag.StringVar(&keep, "keep", "", "message must contain this substring")
	flag.StringVar(&drop, "drop", "", "message must not contain this substring")
	flag.Parse()

	if src == "" || dest == "" {
		flag.Usage()
		os.Exit(1)
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

	if dedup != "" && dupdest == "" {
		log.Fatalln("--dup-dest required if --dedup specified")
	}

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
			if keep != "" && !strings.Contains(*m.Body, keep) {
				log.Println("not keeping message")
				continue
			}

			if drop != "" && strings.Contains(*m.Body, drop) {
				log.Println("dropping message")
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

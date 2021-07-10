package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

var (
	addr   = flag.String("addr", ":8085", "Listen on the TCP network address addr")
	broker = flag.String("broker", "localhost:9092", "Kafka bootstrap servers")

	consumerPoolLimit   = flag.Int("consumerPoolLimit", 5, "")
	messageLimit        = flag.Int("messageLimit", 15, "")
	messageReturnLinger = flag.Duration("messageReturnLinger", time.Second, "")
	producerPoolLimit   = flag.Int("producerPoolLimit", 5, "")
)

func main() {

	flag.Parse()

	http.HandleFunc("/register", registerHandler)
	http.HandleFunc("/send", sendHandler)
	http.HandleFunc("/read", readHandler)

	log.Fatal(http.ListenAndServe(*addr, nil))

}

func registerHandler(w http.ResponseWriter, r *http.Request) {

	user := r.FormValue("user")
	err := RegisterUser(user)
	if err == ErrUserAlreadyExists {
		http.Error(w, "user already exists", http.StatusUnprocessableEntity)
	} else if err != nil {
		log.Printf("Failed to register user: %v", err)
	}

}

func sendHandler(w http.ResponseWriter, r *http.Request) {

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	message := r.FormValue("message")
	from := r.FormValue("from")
	to := r.FormValue("to")
	err := PostMessage(ctx, message, from, to)
	if err != nil {
		log.Println(err)
		somethingWentWrong(w)
	}

}

func readHandler(w http.ResponseWriter, r *http.Request) {

	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()

	forUser := r.FormValue("for")
	messages, err := PullMessages(ctx, forUser)
	if err != nil {
		log.Println(err)
		somethingWentWrong(w)
		return
	}

	for _, message := range messages {
		fmt.Fprintf(w, "%s\n", message)
	}

}

func somethingWentWrong(w http.ResponseWriter) {
	http.Error(w, "something went wrong :-(", http.StatusInternalServerError)
}

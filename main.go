package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/alexbednarczyk/golang-postgres-notify/notifications"

	"github.com/jackc/pgx/v4/pgxpool"
)

const messageChannelBufferSize = 1000

var pool *pgxpool.Pool

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	signalChan := make(chan os.Signal, 1)
	userInputChannel := make(chan []byte, messageChannelBufferSize)

	pool, err := pgxpool.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Unable to connect to database:", err)
		os.Exit(1)
	}
	defer pool.Close()

	go notifications.SendPostgresNotification(ctx, signalChan, userInputChannel, pool)
	go notifications.ListenToPostgresNotifications(ctx, signalChan, pool)

	fmt.Println(`Type a message and press enter.
This message should appear in any other chat instances connected to the same
database.
Type "exit" to quit.`)

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		msg := scanner.Text()
		if msg == "exit" {
			cancel()
			return
		}

		userInputChannel <- []byte(msg)
		if err := scanner.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "Error scanning from stdin:", err)
			return
		}
	}

	signal.Notify(signalChan, os.Interrupt, os.Kill)
	sig := <-signalChan
	cancel()
	fmt.Printf("Exited %s. Received signal: %+v\n", "golang-postgres-notify", sig)
}

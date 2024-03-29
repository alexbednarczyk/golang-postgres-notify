package notifications

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/jackc/pgx/v4/pgxpool"
)

// SendPostgresNotification sends a message from the notificationChannel to Postgres NOTIFY
func SendPostgresNotification(ctx context.Context, wg *sync.WaitGroup, notificationChannel chan []byte, pool *pgxpool.Pool) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-notificationChannel:
			_, err := pool.Exec(context.Background(), "select pg_notify('chat', $1)", msg)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error sending notification:", err)
				return
			}
		}
	}
}

// ListenToPostgresNotifications reads a message from Postgres LISTEN and prints it to the terminal
func ListenToPostgresNotifications(ctx context.Context, wg *sync.WaitGroup, pool *pgxpool.Pool) {
	defer wg.Done()

	conn, err := pool.Acquire(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error acquiring connection:", err)
		return
	}

	defer conn.Release()
	_, err = conn.Exec(context.Background(), "listen chat")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error listening to chat channel:", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			notification, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				return
			}

			fmt.Println("PID:", notification.PID, "Channel:", notification.Channel, "Payload:", notification.Payload)
		}
	}
}

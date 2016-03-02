// +build integration

package recipes

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/betable/ezk"
	"github.com/samuel/go-zookeeper/zk"
)

var testClient *ezk.Client
var acl = zk.WorldACL(zk.PermAll)

func TestLock(t *testing.T) {
	defer testClient.DeleteNodeRecursively("testlock")

	if err := testClient.CreateDir("testlock", acl); err != nil {
		t.Fatal(err)
	}

	l1 := NewLock(testClient, "testlock", acl)
	l2 := NewLock(testClient, "testlock", acl)

	// Simple
	if err := l1.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := l1.Unlock(); err != nil {
		t.Fatal(err)
	}

	ch := make(chan int, 3)

	// Two locks
	if err := l1.Lock(); err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := l2.Lock(); err != nil {
			t.Fatal(err)
		}
		ch <- 2
		if err := l2.Unlock(); err != nil {
			t.Fatal(err)
		}
		ch <- 3
	}()

	time.Sleep(time.Millisecond * 100)

	ch <- 1
	if err := l1.Unlock(); err != nil {
		t.Fatal(err)
	}
	if x := <-ch; x != 1 {
		t.Fatalf("Expected 1 instead of %d", x)
	}
	if x := <-ch; x != 2 {
		t.Fatalf("Expected 2 instead of %d", x)
	}
	if x := <-ch; x != 3 {
		t.Fatalf("Expected 3 instead of %d", x)
	}
}

func TestLockWithCleaner(t *testing.T) {
	defer testClient.DeleteNodeRecursively("testlock")

	if err := testClient.CreateDir("testlock", acl); err != nil {
		t.Fatal(err)
	}

	l1 := NewLock(testClient, "testlock", acl)
	l2 := NewLock(testClient, "testlock", acl)
	l2.WithCleaner(100 * time.Millisecond)

	ch := make(chan int, 3)

	// Two locks
	if err := l1.Lock(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	go func() {
		if err := l2.Lock(); err != nil {
			t.Fatal(err)
		}
		ch <- 1
		if err := l2.Unlock(); err != nil {
			t.Fatal(err)
		}
		ch <- 2
	}()

	if x := <-ch; x != 1 {
		t.Fatalf("Expected 1 instead of %d", x)
	}
	if x := <-ch; x != 2 {
		t.Fatalf("Expected 2 instead of %d", x)
	}
	if err := l1.Unlock(); err != zk.ErrNoNode {
		t.Fatal("Expected zk.ErrNoNode instead of %v", err)
	}
}

func TestTryLock(t *testing.T) {
	defer testClient.DeleteNodeRecursively("testlock")

	if err := testClient.CreateDir("testlock", acl); err != nil {
		t.Fatal(err)
	}

	l1 := NewLock(testClient, "testlock", acl)
	l2 := NewLock(testClient, "testlock", acl)

	// Simple
	if err := l1.TryLock(); err != nil {
		t.Fatal(err)
	}
	if err := l1.Unlock(); err != nil {
		t.Fatal(err)
	}

	ch := make(chan struct{})

	// Two locks
	if err := l1.TryLock(); err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := l2.TryLock(); err != ErrLockFound {
			t.Fatalf("Expected ErrLockFound instead of %v", err)
		}
		ch <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 100)

	if err := l1.Unlock(); err != nil {
		t.Fatal(err)
	}
	// Wait for goroutine
	<-ch
}

func TestTryLockWithCleaner(t *testing.T) {
	defer testClient.DeleteNodeRecursively("testlock")

	if err := testClient.CreateDir("testlock", acl); err != nil {
		t.Fatal(err)
	}

	l1 := NewLock(testClient, "testlock", acl)
	l2 := NewLock(testClient, "testlock", acl)
	l2.WithCleaner(100 * time.Millisecond)

	ch := make(chan struct{})

	// Two locks
	if err := l1.TryLock(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	go func() {
		if err := l2.TryLock(); err != nil {
			t.Fatalf("Expected nil instead of %v", err)
		}
		ch <- struct{}{}
	}()

	// Wait for goroutine
	<-ch

	if err := l1.Unlock(); err != zk.ErrNoNode {
		t.Fatal("Expected zk.ErrNoNode instead of %v", err)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	cfg := ezk.ClientConfig{
		Chroot:  "/ezk",
		Servers: []string{"127.0.0.1:2181"},
	}
	testClient = ezk.NewClient(cfg)
	if err := testClient.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting zookeeper: %s", err.Error())
		os.Exit(1)
	}
	result := m.Run()
	testClient.Close()
	os.Exit(result)
}

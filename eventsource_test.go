package eventsource_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pietjan/eventsource"
)

func TestCreateUser(t *testing.T) {
	store := newMemoryStore()
	es := eventsource.New(&User{}, store,
		eventsource.Bind(&UserCreated{}, &UsernameSet{}, &UserAgeSet{}),
	)

	ctx := context.Background()
	cmd := &CreateUser{
		Model: eventsource.Model{ID: "user1"},
		Name:  "John Doe",
		Age:   30,
	}

	// Apply command to generate events
	err := es.Apply(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}

	// Load the aggregate and verify the state
	user, err := es.Load(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}

	// Verify the state of the aggregate
	if user.ID != "user1" {
		t.Errorf("Expected user ID to be 'user1', got %s", user.ID)
	}

	if user.Name != "John Doe" {
		t.Errorf("Expected user name to be 'John Doe', got %s", user.Name)
	}

	if user.Age != 30 {
		t.Errorf("Expected user age to be 30, got %d", user.Age)
	}

	if user.Version != 1 {
		t.Errorf("Expected user version to be 1, got %d", user.Version)
	}
}

func TestSetUsername(t *testing.T) {
	store := newMemoryStore()
	es := eventsource.New(&User{}, store,
		eventsource.Bind(&UserCreated{}, &UsernameSet{}, &UserAgeSet{}),
	)

	ctx := context.Background()
	cmd1 := &CreateUser{
		Model: eventsource.Model{ID: "user1"},
		Name:  "John Doe",
		Age:   30,
	}
	cmd2 := &SetUsername{
		Model: eventsource.Model{ID: "user1"},
		Name:  "Jane Doe",
	}

	// Apply CreateUser command
	err := es.Apply(ctx, cmd1)
	if err != nil {
		t.Fatal(err)
	}

	// Apply SetUsername command
	err = es.Apply(ctx, cmd2)
	if err != nil {
		t.Fatal(err)
	}

	// Load the aggregate and verify the state
	user, err := es.Load(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}

	// Verify the state of the aggregate
	if user.ID != "user1" {
		t.Errorf("Expected user ID to be 'user1', got %s", user.ID)
	}

	if user.Name != "Jane Doe" {
		t.Errorf("Expected user name to be 'Jane Doe', got %s", user.Name)
	}

	if user.Age != 30 {
		t.Errorf("Expected user age to be 30, got %d", user.Age)
	}

	if user.Version != 2 {
		t.Errorf("Expected user version to be 2, got %d", user.Version)
	}
}

func TestSetUserAge(t *testing.T) {
	store := newMemoryStore()
	es := eventsource.New(&User{}, store,
		eventsource.Bind(&UserCreated{}, &UsernameSet{}, &UserAgeSet{}),
	)

	ctx := context.Background()
	cmd1 := &CreateUser{
		Model: eventsource.Model{ID: "user1"},
		Name:  "John Doe",
		Age:   30,
	}
	cmd2 := &SetUserAge{
		Model: eventsource.Model{ID: "user1"},
		Age:   31,
	}

	// Apply CreateUser command
	err := es.Apply(ctx, cmd1)
	if err != nil {
		t.Fatal(err)
	}

	// Apply SetUserAge command
	err = es.Apply(ctx, cmd2)
	if err != nil {
		t.Fatal(err)
	}

	// Load the aggregate and verify the state
	user, err := es.Load(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}

	// Verify the state of the aggregate
	if user.ID != "user1" {
		t.Errorf("Expected user ID to be 'user1', got %s", user.ID)
	}

	if user.Name != "John Doe" {
		t.Errorf("Expected user name to be 'John Doe', got %s", user.Name)
	}

	if user.Age != 31 {
		t.Errorf("Expected user age to be 31, got %d", user.Age)
	}

	if user.Version != 2 {
		t.Errorf("Expected user version to be 2, got %d", user.Version)
	}
}

func TestConcurrency(t *testing.T) {
	store := newMemoryStore()
	es := eventsource.New(&User{}, store,
		eventsource.Bind(&UserCreated{}, &UsernameSet{}, &UserAgeSet{}),
	)

	ctx := context.Background()
	cmd1 := &CreateUser{
		Model: eventsource.Model{ID: "user1"},
		Name:  "John Doe",
		Age:   30,
	}
	cmd2 := &SetUsername{
		Model: eventsource.Model{ID: "user1"},
		Name:  "Jane Doe",
	}
	cmd3 := &SetUserAge{
		Model: eventsource.Model{ID: "user1"},
		Age:   31,
	}

	// Apply CreateUser command
	err := es.Apply(ctx, cmd1)
	if err != nil {
		t.Fatal(err)
	}

	// Apply SetUsername and SetUserAge commands concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := es.Apply(ctx, cmd2)
		if err != nil {
			t.Error(err)
		}
	}()

	go func() {
		defer wg.Done()
		err := es.Apply(ctx, cmd3)
		if err != nil {
			t.Error(err)
		}
	}()

	wg.Wait()

	// Load the aggregate and verify the state
	user, err := es.Load(ctx, "user1")
	if err != nil {
		t.Fatal(err)
	}

	// Verify the state of the aggregate
	if user.ID != "user1" {
		t.Errorf("Expected user ID to be 'user1', got %s", user.ID)
	}

	if user.Name != "Jane Doe" {
		t.Errorf("Expected user name to be 'Jane Doe', got %s", user.Name)
	}

	if user.Age != 31 {
		t.Errorf("Expected user age to be 31, got %d", user.Age)
	}
}

func TestObserver(t *testing.T) {
	store := newMemoryStore()

	// Create a channel to receive observer notifications
	observerChannel := make(chan eventsource.Event, 10)
	observer := func(event eventsource.Event) {
		observerChannel <- event
	}

	es := eventsource.New(&User{}, store,
		eventsource.Bind(&UserCreated{}, &UsernameSet{}, &UserAgeSet{}),
		eventsource.Observer(observer),
	)

	ctx := context.Background()
	cmd := &CreateUser{
		Model: eventsource.Model{ID: "user1"},
		Name:  "John Doe",
		Age:   30,
	}

	// Apply the command to generate events
	err := es.Apply(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the observer was notified
	select {
	case event := <-observerChannel:
		createdEvent, ok := event.(*UserCreated)
		if !ok {
			t.Errorf("Expected UserCreated event, got %T", event)
		}

		if createdEvent.ID != "user1" {
			t.Errorf("Expected user ID to be 'user1', got %s", createdEvent.ID)
		}

		if createdEvent.Name != "John Doe" {
			t.Errorf("Expected user name to be 'John Doe', got %s", createdEvent.Name)
		}

		if createdEvent.Age != 30 {
			t.Errorf("Expected user age to be 30, got %d", createdEvent.Age)
		}

		if createdEvent.EventVersion() != 1 {
			t.Errorf("Expected event version to be 1, got %d", createdEvent.EventVersion())
		}
	case <-time.After(1 * time.Second): // Timeout after 1 second
		t.Fatal("Expected observer to be notified, but it wasn't")
	}
}

func TestEventStore_Retry(t *testing.T) {
	var saveCalls int
	retryLimit := 3

	// Mock store that fails a few times before succeeding
	store := &mockStore{
		saveFunc: func(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
			saveCalls++
			if saveCalls < retryLimit {
				return errors.New("temporary failure")
			}
			return nil
		},
	}

	es := eventsource.New(&User{}, store,
		eventsource.Bind(&UserCreated{}, &UsernameSet{}, &UserAgeSet{}),
		eventsource.Retry(3, 50*time.Millisecond, time.Second), // Configure retry with max attempts and delay
	)

	ctx := context.Background()
	cmd := &CreateUser{
		Model: eventsource.Model{ID: "user1"},
		Name:  "John Doe",
		Age:   30,
	}

	// Apply the command to generate events
	err := es.Apply(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the store was called the expected number of times
	if saveCalls != retryLimit {
		t.Errorf("Expected save to be called %d times, but it was called %d times", retryLimit, saveCalls)
	}
}

type mockStore struct {
	saveFunc func(ctx context.Context, aggregateID string, records ...eventsource.Record) error
}

func (m *mockStore) Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (eventsource.History, error) {
	// Mock implementation, no need to test Load here
	return nil, nil
}

func (m *mockStore) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {
	if m.saveFunc != nil {
		return m.saveFunc(ctx, aggregateID, records...)
	}
	return nil
}

// User Aggregate
type User struct {
	ID      string
	Version int
	Name    string
	Age     int
}

// UserCreated Event
type UserCreated struct {
	eventsource.Model
	Name string
	Age  int
}

// UsernameSet Event
type UsernameSet struct {
	eventsource.Model
	Name string
}

// UserAgeSet Event
type UserAgeSet struct {
	eventsource.Model
	Age int
}

func (u *User) On(event eventsource.Event) error {
	switch event := event.(type) {
	case *UserCreated:
		u.Version = event.Version
		u.ID = event.ID
		u.Name = event.Name
		u.Age = event.Age

	case *UsernameSet:
		u.Version = event.Version
		u.Name = event.Name

	case *UserAgeSet:
		u.Version = event.Version
		u.Age = event.Age

	default:
		return fmt.Errorf("unknown event type: %T", event)
	}

	return nil
}

type CreateUser struct {
	eventsource.Model
	Name string
	Age  int
}

type SetUsername struct {
	eventsource.Model
	Name string
}

type SetUserAge struct {
	eventsource.Model
	Age int
}

func (u *User) Apply(command eventsource.Command) ([]eventsource.Event, error) {
	eventModel := func(aggregateID string, version int) eventsource.Model {
		return eventsource.Model{ID: aggregateID, Version: version + 1, At: time.Now()}
	}

	switch command := command.(type) {
	case *CreateUser:
		return []eventsource.Event{
			&UserCreated{
				Model: eventModel(command.ID, u.Version),
				Name:  command.Name,
				Age:   command.Age,
			},
		}, nil

	case *SetUsername:
		return []eventsource.Event{
			&UsernameSet{
				Model: eventModel(command.ID, u.Version),
				Name:  command.Name,
			},
		}, nil

	case *SetUserAge:
		return []eventsource.Event{
			&UserAgeSet{
				Model: eventModel(command.ID, u.Version),
				Age:   command.Age,
			},
		}, nil
	default:
		return []eventsource.Event{}, nil
	}
}

func newMemoryStore() eventsource.Store {
	return &memory{
		mutex:   &sync.Mutex{},
		records: make(map[string]eventsource.History),
	}
}

type memory struct {
	mutex   *sync.Mutex
	records map[string]eventsource.History
}

func (m *memory) Load(ctx context.Context, aggregateID string, fromVersion int, toVersion int) (eventsource.History, error) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	all, ok := m.records[aggregateID]
	if !ok {
		return nil, fmt.Errorf("aggegate not found with id %s", aggregateID)
	}

	history := make(eventsource.History, 0, toVersion-fromVersion+1)
	if len(all) > 0 {
		for _, record := range all {
			if v := record.Version; v >= fromVersion && (toVersion == 0 || v <= toVersion) {
				history = append(history, record)
			}
		}
	}

	return history, nil
}

func (m *memory) Save(ctx context.Context, aggregateID string, records ...eventsource.Record) error {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	history, ok := m.records[aggregateID]

	if !ok {
		m.records[aggregateID] = make(eventsource.History, 0)
	}

	history = append(history, records...)

	sort.Slice(history, func(i, j int) bool {
		return history[i].Version < history[j].Version
	})

	m.records[aggregateID] = history

	return nil
}

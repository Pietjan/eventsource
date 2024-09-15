package eventsource

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"time"
)

var (
	ErrInvalidEncoding   = fmt.Errorf("invalid event encoding")
	ErrFailedToDecode    = fmt.Errorf("failed to decode event")
	ErrUnboundEvent      = fmt.Errorf("unbound event type")
	ErrAggregateNotFound = fmt.Errorf("aggregate not found")
	ErrRetryFailed       = fmt.Errorf("retry failed")
	ErrConcurrencyFailed = fmt.Errorf("concurrency failed")
)

type Aggregate interface {
	On(event Event) error
	Apply(command Command) ([]Event, error)
}

type Model struct {
	ID      string
	Version int
	At      time.Time
}

func (m *Model) AggregateID() string { return m.ID }
func (m *Model) EventVersion() int   { return m.Version }
func (m *Model) EventAt() time.Time  { return m.At }

type Event interface {
	AggregateID() string
	EventVersion() int
	EventAt() time.Time
}

type Command interface {
	AggregateID() string
}

type Record struct {
	Version int
	Data    []byte
}

type History []Record

type Store interface {
	Load(ctx context.Context, aggregateID string, fromVersion, toVersion int) (History, error)
	Save(ctx context.Context, aggregateID string, records ...Record) error
}

type EventStoreBase struct {
	store      Store
	serializer *serializer
	observers  []func(event Event)

	// retry options
	retryAttempts int
	retryInitial  time.Duration
	retryMax      time.Duration
}

type Option func(*EventStoreBase)

type EventStore[T Aggregate] struct {
	EventStoreBase
	aggregate T
}

func Bind(events ...Event) Option {
	return func(es *EventStoreBase) {
		for _, event := range events {
			eventType := reflect.TypeOf(event)
			if eventType.Kind() == reflect.Ptr {
				eventType = eventType.Elem()
			}

			es.serializer.eventTypes[eventType.PkgPath()+"."+eventType.Name()] = eventType
		}
	}
}

func Observer(observers ...func(event Event)) Option {
	return func(es *EventStoreBase) {
		es.observers = append(es.observers, observers...)
	}
}

func Retry(attempts int, initial, max time.Duration) Option {
	return func(es *EventStoreBase) {
		es.retryAttempts = attempts
		es.retryInitial = initial
		es.retryMax = max
	}
}

func New[T Aggregate](aggregate T, store Store, options ...Option) *EventStore[T] {
	es := &EventStore[T]{
		EventStoreBase: EventStoreBase{
			store: store,
			serializer: &serializer{
				eventTypes: make(map[string]reflect.Type),
			},

			retryAttempts: 5,
			retryInitial:  100 * time.Millisecond,
			retryMax:      5 * time.Second,
		},
		aggregate: aggregate,
	}

	for _, option := range options {
		option(&es.EventStoreBase)
	}

	return es
}

func (es *EventStore[T]) Save(ctx context.Context, events ...Event) error {
	if len(events) == 0 {
		return nil
	}

	aggregateID := events[0].AggregateID()

	history := make(History, 0, len(events))

	for _, event := range events {
		record, err := es.serializer.encode(event)
		if err != nil {
			return err
		}
		history = append(history, record)
	}

	return retry(es.retryAttempts, es.retryInitial, es.retryMax, func() error {
		return es.store.Save(ctx, aggregateID, history...)
	})
}

func (es *EventStore[T]) Load(ctx context.Context, aggregateID string) (T, error) {
	aggregate, _, err := es.load(ctx, aggregateID)
	return aggregate, err
}

func (es *EventStore[T]) load(ctx context.Context, aggregateID string) (T, int, error) {
	history, err := es.store.Load(ctx, aggregateID, 0, 0)
	if err != nil {
		var zero T
		return zero, 0, err
	}

	if len(history) == 0 {
		var zero T
		return zero, 0, fmt.Errorf("%w, %s", ErrAggregateNotFound, aggregateID)
	}

	aggregate := es.aggregate
	version := 0
	for _, record := range history {
		event, err := es.serializer.decode(record)
		if err != nil {
			var zero T
			return zero, 0, err
		}

		// Apply the event to the aggregate
		if err := aggregate.On(event); err != nil {
			var zero T
			return zero, 0, err
		}

		version = record.Version
	}

	return aggregate, version, nil
}

func (es *EventStore[T]) Apply(ctx context.Context, command Command) error {
	aggregateID := command.AggregateID()

	// Handle empty ID
	if aggregateID == "" {
		return nil
	}

	// Load the aggregate (rehydrate from event history)
	aggregate, version, err := es.load(ctx, aggregateID)
	if err != nil {
		aggregate = es.aggregate
	}

	// Apply the command to generate new events
	events, err := aggregate.Apply(command)
	if err != nil {
		return err
	}

	// Optimistic concurrency check
	expectedVersion := len(events) + version
	currentVersion := events[len(events)-1].EventVersion()

	if expectedVersion != currentVersion {
		return fmt.Errorf("%w: aggregate %s version:  %d != %d", ErrConcurrencyFailed, aggregateID, expectedVersion, currentVersion)
	}

	// Save the new events to the store
	err = es.Save(ctx, events...)
	if err != nil {
		return err
	}

	// Notify observers
	for _, event := range events {
		for _, observer := range es.observers {
			go func(obs func(Event)) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("observer panicked: %v", r)
					}
				}()
				obs(event)
			}(observer)
		}
	}

	return nil
}

type serializer struct {
	eventTypes map[string]reflect.Type
}

type jsonEvent struct {
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
}

func (s *serializer) encode(event Event) (Record, error) {
	eventType := reflect.TypeOf(event)
	if eventType.Kind() == reflect.Ptr {
		eventType = eventType.Elem()
	}

	data, err := json.Marshal(event)
	if err != nil {
		return Record{}, err
	}

	data, err = json.Marshal(jsonEvent{
		Type: eventType.PkgPath() + "." + eventType.Name(),
		Data: json.RawMessage(data),
	})
	if err != nil {
		return Record{}, ErrInvalidEncoding
	}

	return Record{
		Version: event.EventVersion(),
		Data:    data,
	}, nil
}

func (s *serializer) decode(record Record) (Event, error) {
	var data jsonEvent

	err := json.Unmarshal(record.Data, &data)
	if err != nil {
		return nil, ErrFailedToDecode
	}

	t, ok := s.eventTypes[data.Type]
	if !ok {
		return nil, fmt.Errorf("%w, %v", ErrUnboundEvent, data.Type)
	}

	v := reflect.New(t).Interface()
	err = json.Unmarshal(data.Data, v)
	if err != nil {
		return nil, fmt.Errorf("%w, into %#v", ErrFailedToDecode, t)
	}

	return v.(Event), nil
}

func retry(attempts int, initialDelay time.Duration, maxDelay time.Duration, fn func() error) error {
	delay := initialDelay

	for i := 0; i < attempts; i++ {
		err := fn()
		if err == nil {
			return nil
		}

		// If this was the last attempt, return the error
		if i == attempts-1 {
			return fmt.Errorf("%w after %d attempts: %w", ErrRetryFailed, attempts, err)
		}

		// Sleep for the current delay with jitter
		jitter := time.Duration(rand.Int63n(int64(delay / 2)))
		time.Sleep(delay + jitter)

		// Double the delay (exponential backoff)
		delay *= 2

		// Ensure the delay doesn't exceed maxDelay
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return fmt.Errorf("%w after max attempts", ErrRetryFailed)
}

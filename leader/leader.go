/*
Copyright 2016 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leader

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gravitational/coordinate/config"
	"github.com/gravitational/trace"

	ebackoff "github.com/cenkalti/backoff"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client"
)

// Config sets leader election configuration options
type Config struct {
	// ETCD defines etcd configuration, client will be instantiated
	// if passed
	ETCD *config.Config
	// Clock is a time provider
	Clock clockwork.Clock
	// Client is ETCD client will be used if passed
	Client client.Client
}

// Client implements ETCD-backed leader election client
// that helps to elect new leaders for a given key and
// monitors the changes to the leaders
type Client struct {
	client client.Client
	clock  clockwork.Clock
	closeC chan bool
	pauseC chan bool
	closed uint32
}

// NewClient returns a new instance of leader election client
func NewClient(cfg Config) (*Client, error) {
	if cfg.Clock == nil {
		cfg.Clock = clockwork.NewRealClock()
	}
	var err error
	client := cfg.Client
	if client == nil {
		if cfg.ETCD == nil {
			return nil, trace.BadParameter("expected either ETCD config or Client")
		}
		if err = cfg.ETCD.CheckAndSetDefaults(); err != nil {
			return nil, trace.Wrap(err)
		}
		client, err = cfg.ETCD.NewClient()
		if err != nil {
			return nil, trace.Wrap(err)
		}
	}
	return &Client{
		client: client,
		clock:  cfg.Clock,
		closeC: make(chan bool),
		pauseC: make(chan bool),
	}, nil
}

// CallbackFn specifies callback that is called by AddWatchCallback
// whenever leader changes
type CallbackFn func(key, prevValue, newValue string)

// AddWatchCallback adds the given callback to be invoked when changes are
// made to the specified key's value. The callback is called with new and
// previous values for the key. In the first call, both values are the same
// and reflect the value of the key at that moment
func (l *Client) AddWatchCallback(key string, retry time.Duration, fn CallbackFn) {
	go func() {
		valuesC := make(chan string)
		l.AddWatch(key, retry, valuesC)
		var prev string
		for {
			select {
			case <-l.closeC:
				return
			case val := <-valuesC:
				fn(key, prev, val)
				prev = val
			}
		}
	}()
}

func (l *Client) getWatchAtLatestIndex(ctx context.Context, api client.KeysAPI, key string, retry time.Duration) (client.Watcher, *client.Response, error) {
	resp, err := l.getFirstValue(key, retry)
	if err != nil {
		return nil, nil, trace.BadParameter("%v unexpected error: %v", ctx.Value("prefix"), err)
	} else if resp == nil {
		log.Debugf("%v client is closing, return", ctx.Value("prefix"))
		return nil, nil, nil
	}
	log.Debugf("%v got current value '%v' for key '%v'", ctx.Value("prefix"), resp.Node.Value, key)
	watcher := api.Watcher(key, &client.WatcherOptions{
		// Response.Index corresponds to X-Etcd-Index response header field
		// and is the recommended starting point after a history miss of over
		// 1000 events
		AfterIndex: resp.Index,
	})
	return watcher, resp, nil
}

// AddWatch starts watching the key for changes and sending them
// to the valuesC, the watch is stopped
func (l *Client) AddWatch(key string, retry time.Duration, valuesC chan string) {
	prefix := fmt.Sprintf("AddWatch(key=%v)", key)
	api := client.NewKeysAPI(l.client)

	go func() {
		var watcher client.Watcher
		var resp *client.Response
		var err error

		ctx, closer := context.WithCancel(context.WithValue(context.Background(), "prefix", prefix))
		go func() {
			<-l.closeC
			closer()
		}()

		backoff := NewUnlimitedExponentialBackOff()
		ticker := ebackoff.NewTicker(backoff)
		var steps int

		watcher, resp, err = l.getWatchAtLatestIndex(ctx, api, key, retry)
		if err != nil {
			return
		}

		// make sure we always send the first actual value
		if resp != nil && resp.Node != nil {
			select {
			case valuesC <- resp.Node.Value:
			case <-l.closeC:
				return
			}
		}

		var sentAnything bool
		for {

			if watcher == nil {
				watcher, resp, err = l.getWatchAtLatestIndex(ctx, api, key, retry)
			}

			if watcher != nil {
				resp, err = watcher.Next(ctx)
				if err == nil {
					if resp.Node.Value == "" {
						continue
					}
					backoff.Reset()
				}
			}

			if err != nil {
				select {
				case <-ticker.C:
					steps += 1
				}

				if err == context.Canceled {
					return
				} else if cerr, ok := err.(*client.ClusterError); ok {
					if len(cerr.Errors) != 0 && cerr.Errors[0] == context.Canceled {
						return
					}
					log.Debugf("unexpected cluster error: %v (%v)", err, cerr.Detail())
					continue
				} else if IsWatchExpired(err) {
					log.Debug("watch expired, resetting watch index")
					watcher, resp, err = l.getWatchAtLatestIndex(ctx, api, key, retry)
					if err != nil {
						continue
					}
				} else {
					log.Warningf("unexpected watch error: %v", err)
					// try recreating the watch if we get repeated unknown errors
					if steps > 10 {
						watcher, resp, err = l.getWatchAtLatestIndex(ctx, api, key, retry)
						if err != nil {
							continue
						}
						backoff.Reset()
						steps = 0
					}

					continue
				}
			}
			// if nothing has changed and we previously sent this subscriber this value,
			// do not bother subscriber with extra notifications
			if resp.PrevNode != nil && resp.PrevNode.Value == resp.Node.Value && sentAnything {
				continue
			}
			select {
			case valuesC <- resp.Node.Value:
				sentAnything = true
			case <-l.closeC:
				return
			}
		}
	}()
}

// AddVoter starts a goroutine that attempts to set the specified key to
// to the given value with the time-to-live value specified with term.
// The time-to-live value cannot be less than a second.
// After successfully setting the key, it attempts to renew the lease for the specified
// term indefinitely
func (l *Client) AddVoter(context context.Context, key, value string, term time.Duration) error {
	if value == "" {
		return trace.BadParameter("voter value for key cannot be empty")
	}
	if term < time.Second {
		return trace.BadParameter("term cannot be < 1s")
	}
	go func() {
		err := l.elect(key, value, term)
		if err != nil {
			log.Debugf("voter error: %v", err)
		}
		ticker := time.NewTicker(term / 5)
		defer ticker.Stop()
		for {
			select {
			case <-l.pauseC:
				log.Debug("was asked to step down, pausing heartbeat")
				select {
				case <-time.After(term * 2):
				case <-l.closeC:
					return
				case <-context.Done():
					log.Debugf("removing voter for %v", value)
					return
				}
			default:
			}

			select {
			case <-ticker.C:
				err := l.elect(key, value, term)
				if err != nil {
					log.Debugf("voter error: %v", err)
				}
			case <-l.closeC:
				return
			case <-context.Done():
				log.Debugf("removing voter for %v", value)
				return
			}
		}
	}()
	return nil
}

// StepDown makes this participant to pause his attempts to re-elect itself thus giving up its leadership
func (l *Client) StepDown() {
	l.pauseC <- true
}

// getFirstValue returns the current value for key if it exists, or waits
// for the value to appear and loops until client.Close is called
func (l *Client) getFirstValue(key string, retryPeriod time.Duration) (*client.Response, error) {
	api := client.NewKeysAPI(l.client)
	tick := time.NewTicker(retryPeriod)
	defer tick.Stop()
	for {
		resp, err := api.Get(context.TODO(), key, nil)
		if err == nil {
			return resp, nil
		} else if !IsNotFound(err) {
			log.Debugf("unexpected watcher error: %v", err)
		}
		select {
		case <-tick.C:
		case <-l.closeC:
			log.Debug("watcher got client close signal")
			return nil, nil
		}
	}
}

// elect is taken from: https://github.com/kubernetes/contrib/blob/master/pod-master/podmaster.go
// this is a slightly modified version though, that does not return the result
// instead we rely on watchers
func (l *Client) elect(key, value string, term time.Duration) error {
	candidate := fmt.Sprintf("candidate(key=%v, value=%v, term=%v)", key, value, term)
	api := client.NewKeysAPI(l.client)
	resp, err := api.Get(context.TODO(), key, nil)
	if err != nil {
		if !IsNotFound(err) {
			return trace.Wrap(err)
		}
		// try to grab the lock for the given term
		_, err := api.Set(context.TODO(), key, value, &client.SetOptions{
			TTL:       term,
			PrevExist: client.PrevNoExist,
		})
		if err != nil {
			return trace.Wrap(err)
		}
		log.Debugf("%v successfully elected", candidate)
		return nil
	}
	if resp.Node.Value != value {
		return nil
	}
	if resp.Node.Expiration.Sub(l.clock.Now().UTC()) > time.Duration(term/2) {
		return nil
	}

	// extend the lease before the current expries
	_, err = api.Set(context.TODO(), key, value, &client.SetOptions{
		TTL:       term,
		PrevValue: value,
		PrevIndex: resp.Node.ModifiedIndex,
	})
	if err != nil {
		return trace.Wrap(err)
	}
	log.Debugf("%v extended lease", candidate)
	return nil
}

// Close stops current operations and releases resources
func (l *Client) Close() error {
	// already closed
	if !atomic.CompareAndSwapUint32(&l.closed, 0, 1) {
		return nil
	}
	close(l.closeC)
	return nil
}

type ActionCallbackFn func(a Action)

func (l *Client) AddRecursiveWatchCallback(key string, fn ActionCallbackFn) {
	ctx, cancel := context.WithCancel(context.Background())
	valuesC := make(chan Action)
	go func() {
		for {
			select {
			case <-l.closeC:
				cancel()
				return
			case val := <-valuesC:
				fn(val)
			}
		}
	}()
	logger := log.WithField("key", key)
	logger.WithField("peers", l.client.Endpoints()).Info("Setting up watch.")
	go l.watchRecursiveLoop(ctx, key, valuesC, logger)
}

func (l *Client) LeaseLoop(ctx context.Context, key, value string, ttl time.Duration) {
	logger := log.WithFields(logrus.Fields{
		"key":   key,
		"value": value,
		"ttl":   ttl,
	})
	ticker := time.NewTicker(ttl / 5)
	defer func() {
		ticker.Stop()
	}()
	err := l.elect(key, value, ttl)
	if err != nil {
		logger.WithError(err).Warn("Failed to run election term.")
	}
	for {
		select {
		case <-l.pauseC:
			logger.Info("Step down.")
			select {
			case <-ticker.C:
				logger.Info("Resume election participation.")
			case <-l.closeC:
				return
			}
		case <-l.closeC:
			logger.Info("Client is closing.")
			return
		case <-ctx.Done():
			logger.Info("LeaseLoop is closed.")
			return
		case <-ticker.C:
			err := l.elect(key, value, ttl)
			if err != nil {
				logger.WithError(err).Warn("Failed to run election term.")
			}
		}
	}
}

func (l *Client) watchRecursiveLoop(ctx context.Context, key string, valuesC chan Action, logger logrus.FieldLogger) {
	boff := newBackoff()
	// maxFailedSteps sets the limit on the number of failed attempts before the watch
	// is forcibly reset
	const maxFailedSteps = 10
	var (
		api     = client.NewKeysAPI(l.client)
		watcher client.Watcher
	)
	var err error
	for {
		select {
		case <-time.After(boff.NextBackOff()):
		case <-l.closeC:
			logger.Info("Watch loop closing.")
			return
		}
		if watcher == nil {
			watcher, err = l.getRecursiveWatch(ctx, api, key, valuesC, logger)
			if err != nil {
				if IsContextError(err) {
					logger.Info("Context expired, watch loop closing.")
					return
				} else if IsWatchExpired(err) {
					// The watcher has expired, reset it so it's recreated on the
					// next loop cycle.
					logger.Warn("Watch has expired, resetting watch index.")
					watcher = nil
				} else {
					logger.WithError(err).Warn("Failed to create watch at latest index.")
					boff.inc()
					if boff.count() > maxFailedSteps {
						logger.Info("Reset watcher at latest index.")
						watcher = nil
						boff.Reset()
					}
				}
				continue
			}
			//Successful return means the current value has already been sent to receiver
		}
		for {
			resp, err := watcher.Next(ctx)
			if err != nil {
				if IsContextError(err) {
					return
				}
				logger.WithError(err).Warn("Failed to retrieve event from watcher.")
				watcher = nil
				break
			}
			boff.Reset()
			actions := respToActions(resp)
			for _, action := range actions {
				select {
				case valuesC <- action:
				case <-l.closeC:
					logger.Info("Watcher is closing.")
					return
				}
			}
		}
	}
}

func respToActions(resp *client.Response) []Action {
	if resp == nil || resp.Node == nil {
		return nil
	}
	t := toActionType(resp.Action)
	// this is not a update
	if !resp.Node.Dir && t == ActionTypeUpdate &&
		resp.PrevNode != nil &&
		resp.PrevNode.Key == resp.Node.Key &&
		resp.PrevNode.Value == resp.Node.Value {
		return nil
	}
	return nodeToActions(t, resp.Node)
}

func nodeToActions(t ActionType, node *client.Node) []Action {
	if node == nil {
		return nil
	}
	result := make([]Action, 0)
	if !node.Dir {
		result = append(result, Action{
			Type:  t,
			Key:   node.Key,
			Value: node.Value,
		})
		return result
	}
	for _, n := range node.Nodes {
		result = append(result, nodeToActions(t, n)...)
	}
	return result
}

func toActionType(val string) ActionType {
	switch val {
	case "get", "set", "create":
		return ActionTypeCreate
	case "compareAndSwap", "update":
		return ActionTypeUpdate
	case "compareAndDelete", "expire", "delete":
		return ActionTypeDelete
	}
	return ActionType(val)
}

func (l *Client) getRecursiveWatch(ctx context.Context, api client.KeysAPI, key string, valuesC chan Action, logger logrus.FieldLogger) (client.Watcher, error) {
	logger = logger.WithField("key", key)
	logger.Info("Recreating watch at the latest index.")
	resp, err := api.Get(ctx, key, &client.GetOptions{
		Recursive: true,
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	// After reestablishing the watch, always send the value we got to the client.
	if resp.Node != nil {
		logger.WithFields(logrus.Fields{
			"value": resp.Node.Value,
			"index": resp.Index,
		}).Info("Got current value.")
		actions := respToActions(resp)
		for _, action := range actions {
			select {
			case valuesC <- action:
			case <-l.closeC:
				return nil, trace.LimitExceeded("client closed")
			}
		}

	}
	// The watcher that will be receiving events after the value we got above.
	watcher := api.Watcher(key, &client.WatcherOptions{
		Recursive: true,
		// Response.Index corresponds to X-Etcd-Index response header field
		// and is the recommended starting point after a history miss of over
		// 1000 events
		AfterIndex: resp.Index,
	})
	return watcher, nil
}

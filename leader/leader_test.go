package leader

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gravitational/coordinate/config"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client"
	. "gopkg.in/check.v1"
)

func TestLeader(t *testing.T) { TestingT(t) }

type LeaderSuite struct {
	nodes []string
}

var _ = Suite(&LeaderSuite{})

func (s *LeaderSuite) SetUpSuite(c *C) {
	log.SetOutput(os.Stderr)
	nodesString := os.Getenv("COORDINATE_TEST_ETCD_NODES")
	if nodesString == "" {
		// Skips the entire suite
		c.Skip("This test requires etcd, provide comma separated nodes in COORDINATE_TEST_ETCD_NODES environment variable")
		return
	}
	s.nodes = strings.Split(nodesString, ",")
}

func (s *LeaderSuite) newClient(c *C) *Client {
	clt, err := NewClient(
		Config{
			ETCD: &config.Config{
				Endpoints:               s.nodes,
				HeaderTimeoutPerRequest: 100 * time.Millisecond,
			},
		},
	)
	c.Assert(err, IsNil)
	return clt
}

func (s *LeaderSuite) closeClient(c *C, clt *Client) {
	err := clt.Close()
	c.Assert(err, IsNil)
}

func (s *LeaderSuite) TestLeaderElectSingle(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	clt.AddWatchCallback(key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})
	clt.AddVoter(context.TODO(), key, "node1", time.Second)

	select {
	case val := <-changeC:
		c.Assert(val, Equals, "node1")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

func (s *LeaderSuite) TestReceiveExistingValue(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	clt.AddWatchCallback(key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})
	api := client.NewKeysAPI(clt.client)
	_, err := api.Set(context.TODO(), key, "first", nil)
	c.Assert(err, IsNil)

	select {
	case val := <-changeC:
		c.Assert(val, Equals, "first")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

func (s *LeaderSuite) TestLeaderTakeover(c *C) {
	clta := s.newClient(c)

	cltb := s.newClient(c)
	defer s.closeClient(c, cltb)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	cltb.AddWatchCallback(key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})
	clta.AddVoter(context.TODO(), key, "voter a", time.Second)

	// make sure we've elected voter a
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// add voter b to the equation
	cltb.AddVoter(context.TODO(), key, "voter b", time.Second)

	// now, shut down voter a
	c.Assert(clta.Close(), IsNil)
	// in a second, we should see the leader has changed
	time.Sleep(time.Second)

	// make sure we've elected voter b
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

func (s *LeaderSuite) TestLeaderReelectionWithSingleClient(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	clt.AddWatchCallback(key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})
	ctx, cancel := context.WithCancel(context.TODO())
	err := clt.AddVoter(ctx, key, "voter a", time.Second)
	c.Assert(err, IsNil)

	// make sure we've elected voter a
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// add another voter
	clt.AddVoter(context.TODO(), key, "voter b", time.Second)

	// now, shut down voter a
	cancel()
	// in a second, we should see the leader has changed
	time.Sleep(time.Second)

	// make sure we've elected voter b
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

// Make sure leader extends lease in time
func (s *LeaderSuite) TestLeaderExtendLease(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())
	clt.AddVoter(context.TODO(), key, "voter a", time.Second)
	time.Sleep(900 * time.Millisecond)

	api := client.NewKeysAPI(clt.client)
	re, err := api.Get(context.TODO(), key, nil)
	c.Assert(err, IsNil)
	expiresIn := re.Node.Expiration.Sub(time.Now())
	maxTTL := 500 * time.Millisecond
	c.Assert(expiresIn > maxTTL, Equals, true, Commentf("%v > %v", expiresIn, maxTTL))
}

// Make sure we can recover from getting an expired index from our watch
func (s *LeaderSuite) TestHandleLostIndex(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	key := fmt.Sprintf("/planet/tests/index/%v", uuid.New())
	kapi := client.NewKeysAPI(clt.client)

	changeC := make(chan string)
	clt.AddWatchCallback(key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})

	last := ""
	log.Info("setting our key 1100 times")
	for i := 0; i < 1100; i++ {
		val := fmt.Sprintf("%v", uuid.New())
		kapi.Set(context.Background(), key, val, nil)
		last = val
	}

	for {
		select {
		case val := <-changeC:
			log.Infof("got value: %s last: %s", val, last)
			if val == last {
				log.Infof("got expected final value from watch")
				return
			}
		case <-time.After(20 * time.Second):
			c.Fatalf("never got anticipated last value from watch")
		}
	}
}

func (s *LeaderSuite) TestStepDown(c *C) {
	clta := s.newClient(c)
	defer s.closeClient(c, clta)

	cltb := s.newClient(c)
	defer s.closeClient(c, cltb)

	key := fmt.Sprintf("/planet/tests/elect/%v", uuid.New())

	changeC := make(chan string)
	cltb.AddWatchCallback(key, 50*time.Millisecond, func(key, prevVal, newVal string) {
		changeC <- newVal
	})

	// add voter a
	clta.AddVoter(context.TODO(), key, "voter a", time.Second)

	// make sure we've elected voter a
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter a")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}

	// add voter b
	cltb.AddVoter(context.TODO(), key, "voter b", time.Second)

	// tell voter a to step down and wait for the next term
	clta.StepDown()
	time.Sleep(time.Second)

	// make sure voter b is elected
	select {
	case val := <-changeC:
		c.Assert(val, Equals, "voter b")
	case <-time.After(time.Second):
		c.Fatalf("timeout waiting for event")
	}
}

func (s *LeaderSuite) TestLease(c *C) {
	clt := s.newClient(c)
	defer clt.Close()

	prefix := fmt.Sprintf("/planet/tests/%v/lease", uuid.New())
	key1 := fmt.Sprintf("%s/%s", prefix, "master-1")
	key2 := fmt.Sprintf("%s/%s", prefix, "master-2")
	key3 := fmt.Sprintf("%s/%s", prefix, "master-3")

	result := make([]Action, 0)
	clt.AddRecursiveWatchCallback(prefix, func(a Action) {
		result = append(result, a)
	})
	go clt.LeaseLoop(context.TODO(), key1, "master-1", time.Second)
	go clt.LeaseLoop(context.TODO(), key2, "master-2", time.Second)
	go clt.LeaseLoop(context.TODO(), key3, "master-3", time.Second)

	<-time.After(time.Second)
	sortAction(result)
	c.Assert(result, DeepEquals, []Action{
		{
			Type:  ActionTypeCreate,
			Key:   key1,
			Value: "master-1",
		},
		{
			Type:  ActionTypeCreate,
			Key:   key2,
			Value: "master-2",
		},
		{
			Type:  ActionTypeCreate,
			Key:   key3,
			Value: "master-3",
		},
	})
}

func (s *LeaderSuite) TestReceiveRecursiveExistingValue(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	prefix := fmt.Sprintf("/planet/tests/%v/lease", uuid.New())

	key1 := prefix + "/key1"
	key2 := prefix + "/key2"
	key3 := prefix + "/key3"

	result := make([]Action, 0)
	clt.AddRecursiveWatchCallback(prefix, func(a Action) {
		result = append(result, a)
	})
	api := client.NewKeysAPI(clt.client)
	_, err := api.Set(context.TODO(), key1, "value1", nil)
	c.Assert(err, IsNil)
	_, err = api.Set(context.TODO(), key2, "value2", nil)
	c.Assert(err, IsNil)
	_, err = api.Set(context.TODO(), key3, "value3", nil)
	c.Assert(err, IsNil)

	<-time.After(time.Second)
	sortAction(result)
	c.Assert(result, DeepEquals, []Action{
		{
			Type:  ActionTypeCreate,
			Key:   key1,
			Value: "value1",
		},
		{
			Type:  ActionTypeCreate,
			Key:   key2,
			Value: "value2",
		},
		{
			Type:  ActionTypeCreate,
			Key:   key3,
			Value: "value3",
		},
	})
}

func (s *LeaderSuite) TestExpireLease(c *C) {
	clt := s.newClient(c)
	defer s.closeClient(c, clt)

	prefix := fmt.Sprintf("/planet/tests/%v/lease", uuid.New())

	key1 := prefix + "/key1"

	result := make([]Action, 0)
	clt.AddRecursiveWatchCallback(prefix, func(a Action) {
		result = append(result, a)
	})
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second/2)
	defer cancelFunc()
	go clt.LeaseLoop(ctx, key1, "value1", time.Second)

	<-time.After(2 * time.Second)
	sortAction(result)
	c.Assert(result, DeepEquals, []Action{
		{
			Type:  ActionTypeCreate,
			Key:   key1,
			Value: "value1",
		},
		{
			Type:  ActionTypeDelete,
			Key:   key1,
			Value: "",
		},
	})
}

func sortAction(a []Action) {
	sort.SliceStable(a, func(i, j int) bool {
		return a[i].Key < a[j].Key
	})
}

package discovery

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/juju/testing/checkers"
	"github.com/kr/pretty"
	"google.golang.org/grpc/naming"
)

const (
	registryDir = "/elasticell"
	serviceName = "query"
	etcdUrl     = "http://127.0.0.1:2379"
)

var backends = make(map[string]int)
var want = make(map[string]int)

func TestWatcher(t *testing.T) {
	var isEqual bool
	var err error
	var w *EtcdWatcher
	if w, err = NewEtcdWatcher(registryDir, serviceName, []string{etcdUrl}); err != nil {
		t.Fatalf("%+v", err)
	}
	go watchLoop(t, w)

	regs := make(map[int]*EtcdRegistry)
	for i := 0; i < 5; i++ {
		want[getKeyF(i)] = 0
		reg := newRegistry(t, i)
		regs[i] = reg
		go reg.RegisterLoop()
	}
	time.Sleep(10 * time.Second)
	isEqual, err = checkers.DeepEqual(backends, want)
	if !isEqual {
		log.Printf("backends %# v\n, want %# v\n", pretty.Formatter(backends), pretty.Formatter(want))
		t.Fatalf("backends and want %+v", err)
	}

	unregister(regs, 2)
	time.Sleep(15 * time.Second)
	isEqual, err = checkers.DeepEqual(backends, want)
	if !isEqual {
		log.Printf("backends %# v\n, want %# v\n", pretty.Formatter(backends), pretty.Formatter(want))
		t.Fatalf("backends and want %+v", err)
	}

	//w.Close()
	//time.Sleep(10 * time.Second)
}

func newRegistry(t *testing.T, seq int) (reg *EtcdRegistry) {
	var err error
	opt := Option{
		Endpoints:   []string{etcdUrl},
		RegistryDir: registryDir,
		ServiceName: serviceName,
		NodeID:      getKeyF(seq),
		NData: NodeData{
			"foo": "bar",
		},
		TTL: int64(10),
	}
	if reg, err = NewEtcdRegistry(opt); err != nil {
		t.Fatalf("%+v", err)
	}
	return
}

func watchLoop(t *testing.T, w *EtcdWatcher) {
	var err error
	var updates []*naming.Update
	for {
		if updates, err = w.Next(); err != nil {
			t.Fatalf("%+v", err)
		}
		log.Printf("updates: %# v", pretty.Formatter(updates))
		for _, u := range updates {
			switch u.Op {
			case naming.Add:
				backends[u.Addr] = 0
			case naming.Delete:
				delete(backends, u.Addr)
			default:
				t.Fatalf("unrecognized update op: %v", u.Op)
			}
		}
	}
}

func unregister(regs map[int]*EtcdRegistry, seq int) {
	regs[seq].Close()
	delete(want, getKeyF(seq))
}

func getKey(seq int) string {
	return fmt.Sprintf("%s/%s/127.0.0.1:%d", registryDir, serviceName, 8000+seq)
}

func getKeyF(seq int) string {
	return fmt.Sprintf("127.0.0.1:%d", 8000+seq)
}

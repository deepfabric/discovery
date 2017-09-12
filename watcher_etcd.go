package discovery

import (
	"encoding/json"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc/naming"
)

//EtcdWatcher models after grpc.naming.Watcher
type EtcdWatcher struct {
	key       string
	endpoints []string
	cli       *clientv3.Client
	watcher   clientv3.Watcher
	updates   []*naming.Update
	revision  int64
	ctx       context.Context
	cancel    context.CancelFunc
}

//NewEtcdWatcher creates EtcdWatcher
func NewEtcdWatcher(registryDir, serviceName string, endpoints []string) (w *EtcdWatcher, err error) {
	var cli *clientv3.Client
	cli, err = clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}

	key := fmt.Sprintf("%s/%s", registryDir, serviceName)
	watcher := clientv3.NewWatcher(cli)
	ctx, cancel := context.WithCancel(context.Background())

	w = &EtcdWatcher{
		key:       key,
		endpoints: endpoints,
		cli:       cli,
		watcher:   watcher,
		ctx:       ctx,
		cancel:    cancel,
	}
	return
}

//Close cancels the in-flight etcd transaction
func (w *EtcdWatcher) Close() {
	w.cancel()
}

//Next returns current value if it's invoked the first time, otherwise blocks until next update occur
func (w *EtcdWatcher) Next() (updates []*naming.Update, err error) {
	updates = []*naming.Update{}
	if w.revision == 0 {
		var resp *clientv3.GetResponse
		if resp, err = clientv3.NewKV(w.cli).Get(w.ctx, w.key, clientv3.WithPrefix()); err != nil {
			err = errors.Wrap(err, "")
			return
		}
		for _, item := range resp.Kvs {
			nodeData := NodeData{}
			err = json.Unmarshal([]byte(item.Value), &nodeData)
			if err != nil {
				err = errors.Wrap(err, "")
				return
			}
			updates = append(updates, &naming.Update{
				Op:       naming.Add,
				Addr:     string(item.Key)[len(w.key)+1:],
				Metadata: &nodeData,
			})
		}
		//log.Printf("first updates, revision %v\n", resp.Header.Revision)
		w.revision = resp.Header.Revision
		return
	}
	//log.Printf("watching revision %v\n", w.revision+1)
	ch := w.watcher.Watch(w.ctx, w.key, clientv3.WithPrefix(), clientv3.WithRev(w.revision+1))
	select {
	case resp := <-ch:
		err = resp.Err()
		if err != nil {
			return
		}
		//log.Printf("get updates, revision %v\n", resp.Header.Revision)
		//if w.revision+1 != resp.Header.Revision {
		//	log.Printf("revision mismatch, resp: %# v\n", resp)
		//}
		w.revision = resp.Header.Revision
		for _, e := range resp.Events {
			if e.Type == clientv3.EventTypeDelete {
				//e.Kv.Value is []
				updates = append(updates, &naming.Update{
					Op:   naming.Delete,
					Addr: string(e.Kv.Key)[len(w.key)+1:],
				})
			} else if e.IsCreate() || e.IsModify() {
				nodeData := NodeData{}
				if err = json.Unmarshal([]byte(e.Kv.Value), &nodeData); err != nil {
					err = errors.Wrap(err, "")
					return
				}
				updates = append(updates, &naming.Update{
					Op:       naming.Add,
					Addr:     string(e.Kv.Key)[len(w.key)+1:],
					Metadata: &nodeData,
				})
			}
		}
	}
	return
}

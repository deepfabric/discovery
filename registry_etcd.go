package discovery

import (
	"encoding/json"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

//EtcdRegistry registers to etcd
type EtcdRegistry struct {
	endpoints []string
	key       string
	value     string
	ttl       int64
	ctx       context.Context
	cancel    context.CancelFunc
	err       error
}

//Option is the configuration of EtcdRegistry
type Option struct {
	Endpoints   []string //etcd urls
	RegistryDir string
	ServiceName string
	NodeID      string   //key is fmt.Sprintf("%s/%s/%s", option.RegistryDir, option.ServiceName, option.NodeID)
	NData       NodeData //value
	TTL         int64    //TTL of key
}

//NodeData is the struct of value part of registered KV
type NodeData map[string]string

//NewEtcdRegistry creates EtcdRegistry
func NewEtcdRegistry(option Option) (r *EtcdRegistry, err error) {
	var val []byte
	val, err = json.Marshal(option.NData)
	if err != nil {
		err = errors.Wrap(err, "")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	r = &EtcdRegistry{
		endpoints: option.Endpoints,
		key:       fmt.Sprintf("%s/%s/%s", option.RegistryDir, option.ServiceName, option.NodeID),
		value:     string(val),
		ttl:       option.TTL,
		ctx:       ctx,
		cancel:    cancel,
	}
	return
}

//Close cancel RegisterLoop
func (r *EtcdRegistry) Close() {
	r.cancel()
}

//Err returns the error occurred during RegisterLoop
func (r *EtcdRegistry) Err() error {
	return r.err
}

//RegisterLoop keepalives the key in a loop until an error occur or EtcdRegistry be closed
func (r *EtcdRegistry) RegisterLoop() {
	r.err = nil
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: r.endpoints,
	})
	if err != nil {
		r.err = errors.Wrap(err, "")
		return
	}
	defer cli.Close()

	resp, err := cli.Grant(context.TODO(), r.ttl)
	if err != nil {
		r.err = errors.Wrap(err, "")
		return
	}

	_, err = cli.Put(context.TODO(), r.key, r.value, clientv3.WithLease(resp.ID))
	if err != nil {
		r.err = errors.Wrap(err, "")
		return
	}

	// the key will be kept forever
	ch, kaerr := cli.KeepAlive(r.ctx, resp.ID)
	if kaerr != nil {
		r.err = errors.Wrap(err, "")
		return
	}

	for {
		select {
		case ka := <-ch:
			if ka == nil {
				//context canceled
				return
			}
			//log.Printf("e.key: %+v, ka: %+v", r.key, ka)
		}
	}
}

package etcd

import (
	"testing"

	"go-micro.dev/v5/registry"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestWatcherNextSkipsDeleteWithoutPrevKv(t *testing.T) {
	svc := &registry.Service{
		Name:    "foo",
		Version: "1.0.0",
		Nodes:   []*registry.Node{{Id: "foo-1"}},
	}
	key := []byte(nodePath(svc.Name, svc.Nodes[0].Id))

	ch := make(chan clientv3.WatchResponse, 1)
	ch <- clientv3.WatchResponse{Events: []*clientv3.Event{
		{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: key}},
		{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: key}, PrevKv: &mvccpb.KeyValue{Key: key, Value: []byte(encode(svc))}},
	}}
	close(ch)

	w := &etcdWatcher{stop: make(chan bool, 1), w: ch}

	res, err := w.Next()
	if err != nil {
		t.Fatalf("Next() error = %v, want nil", err)
	}
	if res.Action != "delete" {
		t.Errorf("Next() action = %q, want %q", res.Action, "delete")
	}
	if res.Service.Name != svc.Name {
		t.Errorf("Next() service = %q, want %q", res.Service.Name, svc.Name)
	}
}

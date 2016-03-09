package srv

import (
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"testing"
	"time"
)

const (
	DEFAULT_CONNMAP_CAP     = 50
	DEFAULT_CONNSRV_PORT    = 20153
	DEFAULT_CONNSRV_IP_ADDR = ":8087"
)

var (
	gConnM      = NewConnMap(DEFAULT_CONNMAP_CAP)
	serverStart = false
)

func TestListPushFront(t *testing.T) {
	t.Log("TestListPushFront: Start Testing")
	//New empty list
	list := NewConnLRUList()
	if list.Front() != nil {
		t.Error("Should be empty list")
	}

	testCase := []int{1, 3, 89, 24, 45, 78, 7}
	for _, v := range testCase {
		list.PushFront(v)
	}
	if list.Len() != len(testCase) {
		t.Error("The len is dismathed with the case")
	}

	i := len(testCase) - 1
	for q := list.Front(); q != list.Back(); q = q.next {
		if q.Value.(int) != testCase[i] {
			t.Error("The value is not equal expected")
		}
		i--
	}

	t.Log("TestListPushFront: End Testing")
}

func TestListPopFront(t *testing.T) {
	t.Log("TestListPopFront: Start Testing")
	//New empty list
	list := NewConnLRUList()
	if list.PopFront() != nil {
		t.Error("Should be empty front")
	}

	testCase := []int{1, 3, 89, 24, 45, 78, 7}
	for _, v := range testCase {
		list.PushFront(v)
	}

	if list.Len() != len(testCase) {
		t.Error("The len is dismathed with the case")
	}

	i := len(testCase) - 1
	for list.Len() > 0 {
		v := list.PopFront()
		if v.(int) != testCase[i] {
			t.Error("The value is not equal expected")
		}
		i--
	}

	//empty list
	if list.PopFront() != nil {
		t.Error("Empty list should popfront nil")
	}

	t.Log("TestListPopFront: End Testing")
}

func TestListRemove(t *testing.T) {
	t.Log("TestListRemove: Start Testing")
	//New empty list
	list := NewConnLRUList()
	if list.Remove(nil) != nil {
		t.Error("Should be empty")
	}

	testCase := []int{1, 3, 89, 24, 45, 78, 7}
	for _, v := range testCase {
		list.PushFront(v)
	}

	if list.Len() != len(testCase) {
		t.Error("The len is dismathed with the case")
	}

	i := len(testCase) - 1
	for list.Len() > 0 {
		v := list.Remove(list.Front())
		if v.(int) != testCase[i] {
			t.Error("The value is not equal expected")
		}
		i--
	}

	//empty list
	if list.Remove(list.Front()) != nil {
		t.Error("Empty list should nil")
	}

	//Test by reverse order
	for _, v := range testCase {
		list.PushFront(v)
	}

	i = 0
	for list.Len() > 0 {
		v := list.Remove(list.Back())
		if v.(int) != testCase[i] {
			t.Error("The value is not equal expected")
		}
		i++
	}

	t.Log("TestListRemove: End Testing")
}

func TestListPartitionQuick(t *testing.T) {
	//PartitionListQuick(at *LruElement, len int, isSeq bool)
	t.Log("TestListPartitionQuick: Start Testing")
	//New empty list
	list := NewConnLRUList()
	if list.PartitionListQuick(nil, 3, true) != nil {
		t.Error("Should be empty")
	}

	testCase := []int{1, 3, 89, 24, 45, 78, 7}
	for _, v := range testCase {
		list.PushFront(v)
	}

	at := list.Front()
	if at == nil {
		t.Error("The target should be not empty")
	}
	newlist := list.PartitionListQuick(at, 0, true)
	if newlist == nil || newlist.Len() != len(testCase) {
		t.Error("Partition list Error")
	}

	if list.Len() != 0 {
		t.Error("After Partition the list should empty")
	}

	i := len(testCase) - 1
	for newlist.Len() > 0 {
		v := newlist.Remove(newlist.Front())
		if v.(int) != testCase[i] {
			t.Error("The value is not equal expected")
		}
		i--
	}

	//Reverse order
	for _, v := range testCase {
		list.PushFront(v)
	}

	at = list.Back()
	another := list.PartitionListQuick(at, 1, false)
	if another == nil || another.Len() != 1 {
		t.Error("Reverse order Partition error")
	}

	if another.Len() > 0 {
		v := another.Remove(another.Front())
		if v.(int) != testCase[0] {
			t.Error("The value is not equal expected")
		}
	}

	t.Log("TestListPartitionQuick: End Testing")
}

func initTest(t *testing.T) {

	t.Log("Init Testing.......")
	if !serverStart {
		go http.ListenAndServe(":9000", nil)
		serverStart = true
	}

	gConnM.Start()
	var i uint16
	for i = 0; i < DefaultMaxServers-1; i++ {
		err := gConnM.AddServer(i, DEFAULT_CONNSRV_IP_ADDR)
		if err != nil {
			t.Error(err)
			break
		}
	}
}

func TestConnGet(t *testing.T) {
	t.Log("Start testing connect get")
	initTest(t)

	//Out of bound
	t.Log("Out of bound")
	id := uint16(DefaultMaxServers)
	c, err := gConnM.Get(id)
	if err == nil || err.Error() != ERROR_WRONG_SERVER_ID {
		t.Error("Out of bound")
	}

	//Get from no exist server
	t.Log("Get from no exist server")
	id--
	gConnM.DelServer(id)

	c, err = gConnM.Get(id)
	if err == nil || err.Error() != ERROR_NO_EXIST_SERVER {
		t.Error("The server should add first")
	}

	//Normal get: the connect pool is empty
	t.Log("Normal get: the connect pool is empty")
	gConnM.AddServer(id, DEFAULT_CONNSRV_IP_ADDR)
	c, err = gConnM.Get(id)
	if err != nil || c == nil {
		t.Error("Should get a connection correctly :", err, " connection :", c)
	}

	//Normal get: the connect pool is have more connection
	gConnM.Put(id, c)
	c, err = gConnM.Get(id)
	if err != nil || c == nil {
		t.Error("Should get a connection correctly")
	}

	gConnM.Close()
}

func TestConnPut(t *testing.T) {
	initTest(t)

	c, err := net.Dial("tcp", DEFAULT_CONNSRV_IP_ADDR)
	if err != nil {
		t.Error("Create connection is failed!")
		t.Error(err)
	}

	//Id out of bound
	id := uint16(DefaultMaxServers)
	gConnM.Put(id, c)

	//connect c is nil
	t.Log("TestCase: connect c is nil")
	id--
	oldCnt := 0
	if gConnM.cm[id] != nil {
		oldCnt = gConnM.cm[id].list.Len()
	} else {
		gConnM.AddServer(id, DEFAULT_CONNSRV_IP_ADDR)
	}
	gConnM.Put(id, nil)
	if gConnM.cm[id].list.Len() != oldCnt {
		t.Error("nil can not be put in the list")
	}

	//server id is not exist
	t.Log("TestCase: Server id is not exist")
	gConnM.DelServer(id)
	gConnM.Put(id, c)
	if gConnM.cm[id] != nil {
		t.Error("Server not add to the list")
	}

	//server connect pool is empty
	t.Log("TestCase: server connect pool is empty")
	gConnM.DelServer(id)
	gConnM.AddServer(id, DEFAULT_CONNSRV_IP_ADDR)
	gConnM.Put(id, c)
	if gConnM.cm[id].list.Len() != 1 {
		t.Error("Put error happend")
	}

	//server connect pool have connection
	t.Log("TestCase: server connect pool have connection")
	normalPutCnt := 10
	oldCnt = gConnM.cm[id].list.Len()
	for i := 0; i < normalPutCnt; i++ {
		gConnM.Put(id, c)
	}
	if gConnM.cm[id].list.Len() != (oldCnt + normalPutCnt) {
		t.Error("Normal put error happend")
	}

	//put more connection
	t.Log("TestCase: server connect pool have more connection")
	oldCnt = gConnM.cm[id].list.Len()
	moreCnt := 2000
	for i := 0; i < moreCnt; i++ {
		gConnM.Put(id, c)
	}

	time.Sleep(5 * time.Second)

	if gConnM.sharedConnLru.Len() > DEFAULT_CONNMAP_CAP {
		t.Error("The pool count should smaller than the capcity,current count is ", gConnM.sharedConnLru.Len())
	}

	if gConnM.cm[id].list.Len() > DEFAULT_CONNMAP_CAP {
		t.Error("The sub pool count should smaller than the capcity,current count is ", gConnM.cm[id].list.Len())
	}

	gConnM.DelServer(id)
	gConnM.Close()
}

func TestAddServer(t *testing.T) {
	initTest(t)

	id := uint16(len(gConnM.cm) - 1)
	//normal add
	err := gConnM.AddServer(id, DEFAULT_CONNSRV_IP_ADDR)
	if err != nil {
		t.Error("Add server failed")
	}

	//add the same id
	err = gConnM.AddServer(id, DEFAULT_CONNSRV_IP_ADDR)
	if err != nil {
		t.Error("Add same server id failed")
	}

	//add the same id with different ip address
	err = gConnM.AddServer(id, ":"+strconv.Itoa(DEFAULT_CONNSRV_PORT+1))
	if err == nil || err.Error() != ERROR_CONFLICT_SERVER_INFO {
		t.Error("Add same server id with different test failed")
	}

	//add server total count beyond limited
	for ; id < DefaultMaxServers; id++ {
		err = gConnM.AddServer(id, DEFAULT_CONNSRV_IP_ADDR)
		if err != nil {
			t.Error("Add Server failed")
			break
		}
	}

	id++
	err = gConnM.AddServer(id, DEFAULT_CONNSRV_IP_ADDR)
	if err == nil || err.Error() != ERROR_WRONG_SERVER_ID {
		t.Error("Should add error")
	}

	t.Log("TestAddServer close")
	gConnM.Close()
}

func TestDelServer(t *testing.T) {
	initTest(t)

	id := uint16(len(gConnM.cm) - 1)
	//normal add
	t.Log("Normal add server")
	gConnM.AddServer(id, DEFAULT_CONNSRV_IP_ADDR)

	//Delete id not exist
	id++
	t.Log("Delete id not exist")
	gConnM.DelServer(id)

	t.Log("Delete all id server")
	for ; id >= 0; id-- {
		gConnM.DelServer(id)
		if id == 0 {
			break
		}
	}

	gConnM.Close()
}

func TestAddAndDelServer(t *testing.T) {
	maxlen := 2 * len(gConnM.cm)
	stop := false

	gConnM.Start()
	//Del Server
	go func() {
		rand.Seed(time.Now().UnixNano())
		for {
			if stop {
				break
			}

			index := rand.Intn(maxlen)
			gConnM.DelServer(uint16(index))
		}

	}()

	//Add Server
	go func() {
		rand.Seed(time.Now().UnixNano())
		for {
			if stop {
				break
			}

			index := rand.Intn(maxlen)
			gConnM.AddServer(uint16(index), DEFAULT_CONNSRV_IP_ADDR)
		}
	}()

	select {
	case <-time.After(3 * time.Second):
		stop = true
	}

	time.Sleep(time.Second)

	cnt := 0
	for _, v := range gConnM.cm {
		if v != nil {
			cnt++
		}
	}

	t.Log("The server left is ", cnt)
	if cnt == len(gConnM.cm) || cnt == 0 {
		t.Error("Maybe sth wrong")
	}

	gConnM.Close()
}

func TestClose(t *testing.T) {
	t.Log("Start Test Close......")
	initTest(t)

	//Put some connect
	c, err := net.Dial("tcp", DEFAULT_CONNSRV_IP_ADDR)
	if err != nil {
		t.Log(err)
	}
	testCnt := DefaultMaxServers * DEFAULT_CONNMAP_CAP
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < testCnt; i++ {
		gConnM.Put(uint16(rand.Intn(DefaultMaxServers)), c)
	}

	t.Log("TestClose() ready to close the connect map")
	gConnM.Close()

	t.Log("TestClose check gConnM")
	for _, v := range gConnM.cm {
		if v != nil {
			t.Error("Close ConnMap failed")
			break
		}
	}

	id := uint16(1)

	//Get should unavaliable
	t.Log("TestClose Get should unavaliable")
	c, err = gConnM.Get(id)
	if err == nil || err.Error() != ERROR_CONNPOOL_UNAVALIABLE {
		t.Error("Get should be unavaliable")
	}
	t.Log("TestClose Test finished")
}

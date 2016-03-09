package srv

import (
	"errors"
	"net"
	"sync"
	"time"
)

//a map from server id to connection pool for peer-to-peer or client-to-server communication

const DefaultMaxConnections = 20000
const DefaultMaxServers = 10000
const DefaultConnectionThresholdRate = 0.9
const DefaultShrinkSpan = 100 //Millisecond

const (
	ERROR_ADD_MORE_SERVER      = "MoreServer"
	ERROR_NO_EXIST_SERVER      = "NotExist"
	ERROR_CONFLICT_SERVER_INFO = "ConflictServerInfo"
	ERROR_IP_PORT_EMPTY        = "EmptyIp"
	ERROR_WRONG_SERVER_ID      = "WrongServerId"
	ERROR_UNKNOWN              = "UnkownError"
	ERROR_CONNPOOL_UNAVALIABLE = "UnAvaliable"
)

//LRU Element
type LruElement struct {
	next, prev *LruElement
	//The value stored in the element
	Value interface{}
}

//Double link list,should support split and insert from front
type ConnLRUList struct {
	root LruElement
	len  int
}

//New a connect LRU list and init it
func NewConnLRUList() *ConnLRUList {
	return new(ConnLRUList).InitConnLRUList()
}

//Init a connect list
func (cl *ConnLRUList) InitConnLRUList() *ConnLRUList {
	cl.root.next = &cl.root
	cl.root.prev = &cl.root
	cl.len = 0
	return cl
}

//The length of the connect LRU list
func (cl *ConnLRUList) Len() int {
	return cl.len
}

//Update the length of the list
func (cl *ConnLRUList) SetLen(len int) {
	if len >= 0 {
		cl.len = len
	}
}

//The front element of the connect LRU list
func (cl *ConnLRUList) Front() *LruElement {
	if cl.len > 0 {
		return cl.root.next
	}

	return nil
}

func (cl *ConnLRUList) Back() *LruElement {
	if cl.len > 0 {
		return cl.root.prev
	}

	return nil
}

//Push sth to the list
func (cl *ConnLRUList) PushFront(v interface{}) {
	le := &LruElement{Value: v}
	fn := cl.root.next
	fn.prev = le
	le.next = fn
	le.prev = &cl.root
	cl.root.next = le
	cl.len++
}

//Pop the front element and return the value in the element
func (cl *ConnLRUList) PopFront() interface{} {
	if cl.len > 0 {
		ve := cl.Front()
		cl.root.next = ve.next
		ve.next.prev = &cl.root
		ve.next = nil
		ve.prev = nil

		cl.len--
		return ve.Value
	}

	return nil
}

//Remove the specified element and return the value in it
func (cl *ConnLRUList) Remove(pos *LruElement) interface{} {
	if pos != nil && cl.len > 0 {
		pos.prev.next = pos.next
		pos.next.prev = pos.prev

		pos.next = nil
		pos.prev = nil

		cl.len--
		return pos.Value
	}

	return nil
}

//Just update LRU list information with cut the list by position at
func (cl *ConnLRUList) PartitionListQuick(at *LruElement, len int, isSeq bool) *ConnLRUList {
	if at == nil || len < 0 {
		return nil
	}

	pcl := NewConnLRUList()
	pcl.root.next = at
	pcl.root.prev = cl.root.prev
	pcl.root.prev.next = &pcl.root

	cl.root.prev = at.prev
	at.prev.next = &cl.root

	at.prev = &pcl.root

	if isSeq {
		pcl.len = cl.len - len
		cl.len = len
	} else {
		pcl.len = len
		cl.len = cl.len - len
	}

	return pcl
}

//Partition the list in the specified position
func (cl *ConnLRUList) PartitionList(pos int, isSeq bool) *ConnLRUList {
	if pos < 0 || cl.len < pos {
		return nil
	}

	p := &cl.root
	for i := 0; i < pos; i++ {
		if isSeq {
			p = p.next
		} else {
			p = p.prev
		}
	}

	pcl := cl.PartitionListQuick(p, pos, isSeq)

	return pcl
}

type ConnMap struct {
	lock          sync.Mutex
	cm            [DefaultMaxServers]*ConnPool
	sharedConnLru *ConnLRUList
	//capacity
	capacity int
	//is avaliable
	isAvaliable bool
	//check shrinking stat
	isShrinking bool
	//shrink deamon
	shrinkDeamonRunning bool
	//channel for notified the deamon
	shrinkChan chan bool
}

type ConnPoolElement struct {
	SrvPool *ConnPool
	Conn    net.Conn
}

//Single server connect pool
type ConnPool struct {
	lock sync.Mutex
	id   uint16
	addr string
	list *ConnLRUList
}

func newConnPool(id uint16, ip string) *ConnPool {
	return &ConnPool{
		id:   id,
		addr: ip,
		list: NewConnLRUList(),
	}
}

//Get idle connection count
func (cp *ConnPool) getIdleCnt() int {
	return cp.list.Len()
}

//Put connection to the idle list
func (cp *ConnPool) put(v interface{}) {
	cp.list.PushFront(v)
}

//Get one idle connection
func (cp *ConnPool) get() interface{} {
	if cp.list == nil || cp.list.Len() == 0 {
		return nil
	}

	return cp.list.PopFront()
}

func NewConnMap(capx int) *ConnMap {
	if capx > DefaultMaxConnections {
		capx = DefaultMaxConnections
	}

	return &ConnMap{
		capacity:      capx,
		isShrinking:   false,
		isAvaliable:   false,
		sharedConnLru: NewConnLRUList(),
	}
}

func (p *ConnMap) Start() {
	p.lock.Lock()
	p.isAvaliable = true

	if p.shrinkDeamonRunning == false {
		p.shrinkChan = make(chan bool)
		go p.shrinkDaemon()
		p.shrinkDeamonRunning = true
	}

	p.lock.Unlock()
}

//Get specified server connection pool
func (p *ConnMap) Get(id uint16) (c net.Conn, err error) {
	if !p.isAvaliable {
		err = errors.New(ERROR_CONNPOOL_UNAVALIABLE)
		return
	}

	if id >= DefaultMaxServers {
		err = errors.New(ERROR_WRONG_SERVER_ID)
		return
	}

	p.lock.Lock()
	cp := p.cm[id]
	if cp == nil {
		p.lock.Unlock()
		err = errors.New(ERROR_NO_EXIST_SERVER)
		return
	}

	cp.lock.Lock()
	p.lock.Unlock()
	index := cp.get()

	p.lock.Lock()
	cp.lock.Unlock()
	if index == nil {
		p.lock.Unlock()
		//new one connection
		c, err = net.Dial("tcp", cp.addr)
		return
	}

	ce := p.sharedConnLru.Remove(index.(*LruElement))
	p.lock.Unlock()
	if ce != nil {
		c = ce.(*ConnPoolElement).Conn
		return c, nil
	}

	err = errors.New(ERROR_UNKNOWN)
	return
}

//Put connection to the specified server pool
func (p *ConnMap) Put(id uint16, c net.Conn) {
	if c == nil {
		return
	}

	if !p.isAvaliable || id >= DefaultMaxServers {
		c.Close()
		return
	}

	p.lock.Lock()
	cp := p.cm[id]
	if cp == nil {
		p.lock.Unlock()
		c.Close()
		return
	}
	p.lock.Unlock()

	cpe := &ConnPoolElement{
		SrvPool: cp,
		Conn:    c,
	}
	p.lock.Lock()
	p.sharedConnLru.PushFront(cpe)
	clf := p.sharedConnLru.Front()
	cp.lock.Lock()
	p.lock.Unlock()
	cp.list.PushFront(clf)
	cp.lock.Unlock()

	//Whether need to shrink
	if p.sharedConnLru.Len() > int(float64(p.capacity)*DefaultConnectionThresholdRate) {
		go func() {
			//check channel if closed
			select {
			case _, isok := <-p.shrinkChan:
				if !isok {
					//Channel closed
					return
				}

			//channel is empty
			default:
			}

			select {
			//Send shrink signal
			case p.shrinkChan <- true:
			//channel is full
			default:
			}
		}()
	}
}

//Add specified server , create connection pool
func (p *ConnMap) AddServer(id uint16, ipPort string) (err error) {
	if !p.isAvaliable {
		return errors.New(ERROR_CONNPOOL_UNAVALIABLE)
	}

	if len(ipPort) == 0 {
		return errors.New(ERROR_IP_PORT_EMPTY)
	}

	if id >= DefaultMaxServers {
		return errors.New(ERROR_WRONG_SERVER_ID)
	}

	p.lock.Lock()
	cp := p.cm[id]
	// already exist
	if cp != nil {
		p.lock.Unlock()
		if id == cp.id && cp.addr == ipPort {
			return
		}

		return errors.New(ERROR_CONFLICT_SERVER_INFO)
	}

	cp = newConnPool(id, ipPort)
	p.cm[id] = cp
	p.lock.Unlock()
	return
}

//Del specified server
func (p *ConnMap) DelServer(id uint16) {
	if !p.isAvaliable {
		return
	}

	if id >= DefaultMaxServers {
		return
	}

	p.lock.Lock()
	cp := p.cm[id]
	if cp == nil {
		p.lock.Unlock()
		return
	}

	p.cm[id] = nil
	p.lock.Unlock()

	go p.CloseConnPool(cp)
}

//Shrink the connect pool and return the last pos should be split
func (p *ConnMap) findShrinkPos(needShrinkCnt int) (lastpos *LruElement, actualpos int) {
	if p.sharedConnLru.Len() == 0 || needShrinkCnt > p.sharedConnLru.Len() {
		return
	}

	p.lock.Lock()
	//The Zero position
	lastpos = p.sharedConnLru.Front().prev
	actualpos = 0
	for ; actualpos < needShrinkCnt && p.sharedConnLru.Len() > 0; actualpos++ {
		lastpos = lastpos.prev
		if lastpos == nil {
			break
		}

		if lastpos.Value == nil {
			continue
		}

		//Remove the last of the connect pool list
		kcp := lastpos.Value.(*ConnPoolElement).SrvPool
		kcp.lock.Lock()
		p.lock.Unlock()
		back := kcp.list.Back()
		if back != nil && back.Value == lastpos {
			kcp.list.Remove(kcp.list.Back())
		}
		p.lock.Lock()
		kcp.lock.Unlock()
	}
	p.lock.Unlock()

	return lastpos, actualpos
}

//Shrink daemon for shrink connnect pool
func (p *ConnMap) shrinkDaemon() {
	for {
		select {
		//Receive shrink signal
		case _, ok := <-p.shrinkChan:
			//Channel close
			if !ok {
				break
			}
		//Time out for shrink
		case <-time.After(DefaultShrinkSpan * time.Millisecond):
		}

		p.shrink()
	}
}

//Close connection in the list
func (p *ConnMap) closeAllConn(clearList *ConnLRUList) {
	for clearList.Len() > 0 {
		ce := clearList.PopFront()
		if ce != nil {
			c := ce.(*ConnPoolElement).Conn
			c.Close()
		}
	}
}

//Shrink the global connection pool
func (p *ConnMap) shrink() {
	if !p.isAvaliable || p.isShrinking {
		return
	}

	p.lock.Lock()
	if p.isShrinking {
		p.lock.Unlock()
		return
	}

	//Shrink to threshold
	needShrinkCnt :=
		p.sharedConnLru.Len() - int(float64(p.capacity)*DefaultConnectionThresholdRate)

	if needShrinkCnt <= 0 {
		p.isShrinking = false
		p.lock.Unlock()
		return
	}

	p.isShrinking = true
	p.lock.Unlock()

	//Reverse traversal the global LRU list
	//and mark the position should be cut off in the conn pool
	lastpos, actual := p.findShrinkPos(needShrinkCnt)
	if lastpos == nil || actual == 0 {
		//set isshrinking
		p.lock.Lock()
		p.isShrinking = false
		p.lock.Unlock()
		return
	}

	//Cut off the lru list
	p.lock.Lock()
	clearList := p.sharedConnLru.PartitionListQuick(lastpos, actual, false)

	p.isShrinking = false
	p.lock.Unlock()

	//Close all connection already shrink
	if clearList != nil && clearList.Len() > 0 {
		go p.closeAllConn(clearList)
	}
}

//Close specified connection pool
func (p *ConnMap) CloseConnPool(cp *ConnPool) {
	if cp == nil || cp.list == nil {
		return
	}

	cp.lock.Lock()
	for cp.list.Len() > 0 {
		index := cp.list.PopFront()
		if index == nil {
			continue
		}

		p.lock.Lock()
		cp.lock.Unlock()
		ce := p.sharedConnLru.Remove(index.(*LruElement))
		p.lock.Unlock()
		if ce != nil {
			c := ce.(*ConnPoolElement).Conn
			c.Close()
		}

		cp.lock.Lock()
	}

	cp.lock.Unlock()
}

//Close whole connection pool
func (p *ConnMap) Close() {
	p.lock.Lock()

	//unavaliable
	p.isAvaliable = false

	for i, _ := range p.cm {
		//Clear the connect pool first
		cp := p.cm[i]
		if cp == nil {
			continue
		}
		p.cm[i] = nil
	}

	if p.sharedConnLru.Len() > 0 {
		//clear all connnection
		cleanLru := p.sharedConnLru.PartitionListQuick(p.sharedConnLru.Front(), 0, true)
		go p.closeAllConn(cleanLru)
	}

	p.isShrinking = false

	p.lock.Unlock()
}

//Close all connection and release source
func (p *ConnMap) ShutDown() {
	if p.isAvaliable {
		p.Close()
	}

	close(p.shrinkChan)
}

package main

import (
	"sync"
	"time"

	"github.com/bitly/nsq/nsqd"
)

var round time.Duration = 2 * time.Second

type Host struct {
	host                                    string
	lastHeardFromAt                         time.Time
	buckets                                 map[time.Time]*Bucket
	nextBucket                              *Bucket
	recoveryLock, bucketsLock, messagesLock *sync.Mutex
	messages                                map[nsqd.MessageID]*Bucket
	inRecovery                              bool
}

func NewHost(hostname string) *Host {
	h := &Host{
		host:         hostname,
		buckets:      map[time.Time]*Bucket{},
		bucketsLock:  &sync.Mutex{},
		messagesLock: &sync.Mutex{},
		recoveryLock: &sync.Mutex{},
		messages:     map[nsqd.MessageID]*Bucket{},
	}
	h.nextBucket = h.GetBucketAtExpireTime(time.Now())
	return h
}

type Bucket struct {
	messages   map[nsqd.MessageID]nsqd.Message
	expiration time.Time
	next       *Bucket
	host       *Host
}

func (b *Bucket) GetMessage(id nsqd.MessageID) nsqd.Message {
	return b.messages[id]
}

func (h *Host) GetBucketAtExpireTime(e time.Time) *Bucket {
	rounded := e.Round(round)

	h.bucketsLock.Lock()
	b, ok := h.buckets[rounded]
	h.bucketsLock.Unlock()

	if !ok {
		b = &Bucket{
			messages:   make(map[nsqd.MessageID]nsqd.Message),
			expiration: e,
		}

		// is this bucket the earliest?
		if e.Before(h.nextBucket.expiration) {
			b.next = h.nextBucket
			h.nextBucket = b
		}

		// what is the previous?
		prev := h.GetBucketAtExpireTime(rounded.Add(-round))
		prev.next = b

		h.bucketsLock.Lock()
		h.buckets[rounded] = b
		h.bucketsLock.Unlock()
	}
	return b
}

func (h *Host) AddMessage(m nsqd.Message, e time.Time) {
	if h == nil {
		return
	}
	h.RemoveMessage(m)
	h.GetBucketAtExpireTime(e).messages[m.ID] = m
}

func (h *Host) RemoveMessage(m nsqd.Message) {
	if h == nil {
		return
	}
	h.messagesLock.Lock()
	defer h.messagesLock.Unlock()
	if have, ok := h.messages[m.ID]; ok {
		delete(have.messages, m.ID)
		delete(h.messages, m.ID)
	}
}

func (h *Host) Recovery(stop chan bool) {
	if h == nil {
		return
	}
	ticker := time.NewTicker(round)
	for {
		select {
		case <-ticker.C:
			expire := h.nextBucket
			h.nextBucket = expire.next
			expire.Expire()
		case <-stop:
			return
		}
	}
}

func (b *Bucket) Expire() {
	if len(b.messages) > 0 {
		go b.host.InitiateRecovery()
	}
}

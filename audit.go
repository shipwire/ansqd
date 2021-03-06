package main

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/bitly/nsq/nsqd"
	"github.com/hashicorp/serf/command/agent"
	"github.com/shipwire/ansqd/internal/polity"
)

var ExpirationTime = 2 * time.Minute

type audit struct {
	m *nsqd.Message
}

type delegate struct{}

// OnFinish is called when FIN is received for the message
func (d *delegate) OnFinish(m *nsqd.Message) {
	n.GetTopic("audit.finish").PutMessage(&nsqd.Message{
		ID:   n.NewID(),
		Body: m.ID[:],
	})
	log.Printf("AUDIT: OnFinish %x", m.ID)
}

// OnQueue is called before a message is sent to the queue
func (d *delegate) OnQueue(m *nsqd.Message, topic string) {
	if strings.HasPrefix(topic, "audit.") {
		return
	}
	n.GetTopic("audit.send").PutMessage(&nsqd.Message{
		ID:   n.NewID(),
		Body: auditMessage{*m, topic}.Bytes(),
	})
	log.Printf("AUDIT: OnQueue %x", m.ID)
}

// OnRequeue is called when REQ is received for the message
func (d *delegate) OnRequeue(m *nsqd.Message, delay time.Duration) {
	a.Req(m)
}

// OnTouch is called when TOUCH is received for the message
func (d *delegate) OnTouch(m *nsqd.Message) {
	a.Touch(m)
}

type auditor struct {
	p         *polity.Polity
	ag        *agent.Agent
	hosts     map[string]*Host
	hostsLock *sync.Mutex
}

func (a auditor) Audit(m *nsqd.Message) {
	a.ExtractHost(m).AddMessage(*m, time.Now().Add(ExpirationTime))
}

func (a auditor) Fin(m *nsqd.Message) {
	a.ExtractHost(m).RemoveMessage(*m)
}

func (a auditor) Req(m *nsqd.Message) {
	a.ExtractHost(m).AddMessage(*m, time.Now().Add(ExpirationTime))
}

func (a auditor) Touch(m *nsqd.Message) {
	a.ExtractHost(m).AddMessage(*m, time.Now().Add(ExpirationTime))
}

func (h *Host) InitiateRecovery() {
	h.recoveryLock.Lock()
	if h.inRecovery {
		return
	}
	h.inRecovery = true
	h.recoveryLock.Unlock()

	role := "recover:" + h.host
	err := <-a.p.RunElection(role)
	defer a.p.RunRecallElection(role)
	if err != nil {
		return
	}

	for mid, bucket := range h.messages {
		m := bucket.GetMessage(mid)
		am := extractAudit(m)
		n.GetTopic(am.Topic).PutMessage(&am.Message)
	}
}

func (a auditor) ExtractHost(m *nsqd.Message) *Host {
	body := map[string]interface{}{}
	err := json.Unmarshal(m.Body, &body)
	if err != nil {
		return nil
	}

	var hostname string
	if h, ok := body["hostname"]; !ok {
		return nil
	} else {
		hostname, ok = h.(string)
		if !ok {
			return nil
		}
	}

	return a.GetHost(hostname)
}

func (a auditor) GetHost(hostname string) *Host {
	a.hostsLock.Lock()
	defer a.hostsLock.Unlock()

	host, ok := a.hosts[hostname]
	if !ok {
		host = NewHost(hostname)
		a.hosts[hostname] = host
	}

	return host
}

type auditMessage struct {
	nsqd.Message
	Topic string
}

func (a auditMessage) Bytes() []byte {
	return nil
}

func extractAudit(m nsqd.Message) auditMessage {
	return auditMessage{}
}

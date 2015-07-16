package main

import (
	"fmt"
	"testing"
)

var testTopicMap = map[string]string{
	"a_*.log": "a_topic",
	"b_*.log": "b_topic",
}

func TestGenTopic(t *testing.T) {
	path1 := "a_123.log"
	path2 := "m_a_123.log"
	topic1 := genTopic(path1, testTopicMap)
	if topic1 == "" || topic1 != "a_topic" {
		fmt.Println(path1, topic1, topic1 == "a_topic")
		t.Fail()
	}
	topic2 := genTopic(path2, testTopicMap)
	fmt.Println(topic2)
	if topic2 != "" || topic2 == "a_topic" {
		fmt.Println(path2, topic2)
		t.Fail()
	}
}

func TestGetSourceName(t *testing.T) {
	msg1 := "1 a_1.log message aaa"
	msg2 := "0 b_1.log message bbb"
	name1 := getSourceName(msg1)
	if name1 == "" || name1 != "a_1.log" {
		t.Fail()
	}
	name2 := getSourceName(msg2)
	if name2 == "" || name2 != "b_1.log" {
		t.Fail()
	}
}

func TestMatchTopic(t *testing.T) {
	path1 := "a_123.log"
	path2 := "m_123.log"

	m1 := matchTopic(path1, testTopicMap)
	m2 := matchTopic(path2, testTopicMap)

	if m1 == false || m2 == true {
		t.Fail()
	}

	emptyTopic := map[string]string{}
	m1 = matchTopic(path1, emptyTopic)
	m2 = matchTopic(path2, emptyTopic)

	if m1 == false || m2 == false {
		t.Fail()
	}
}

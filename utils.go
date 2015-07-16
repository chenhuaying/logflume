package main

import (
	"path/filepath"
	"strings"
)

func genTopic(name string, topicmap map[string]string) string {
	for k, v := range topicmap {
		match, _ := filepath.Match(k, name)
		if match {
			return v
		}
	}
	return ""
}

func getSourceName(message string) string {
	idx := strings.Index(message[2:], " ")
	if idx != -1 {
		return message[2 : 2+idx]
	}
	return ""
}

func matchTopic(name string, topicmap map[string]string) bool {
	if len(topicmap) > 0 {
		topic := genTopic(name, topicmap)
		if topic == "" {
			return false
		} else {
			return true
		}
	} else {
		return true
	}
}

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/docopt/docopt-go"
)

var checkpoint string
var (
	cpus              = 2
	daemon_mode       = false
	work_dir          = "./logflume_worker"
	tailOnLog         = false
	harvestBufferSize = 16 << 10 //16k
	deadtime          = "1h"     //1 hour
	defaultTopic      = "test"   //kafka Topic
	defaultBrokerList = []string{"127.0.0.1:9092"}
	kafkabuffer       = 256
	topicmap          = map[string]string{}
	starttime         = "1h"
)

var kafkaTopic = defaultTopic
var retryTopic = kafkaTopic
var hashKey = "test-key"
var brokerList = defaultBrokerList

var usage = `usage: logflume (--checkpoint=<path> | -c=<path>) [options]

Options:
 -h, --help    Help info
 -d, --daemon    Daemon mode[default: false]
 -w, --work-dir=<path>    Work directory[default: logflume_worker]
 -c, --checkpoint=<path>    Check point, directory or an file, terminal with / means a directory
 -k, --cpus=<num>    Num of CPU logflume use
 -l, --log-level=<level>    Log level, default info, panic | fatal | error | warn | info | debug
 -t, --tail=<flag>    Tail on Log
 --deadtime=<time>    The time between last modify, set log to dead[default: 1h](m:Minute, h:Hour)
 --topic=<topic>	kafka Topic
 --retrytopic=<retry> retry Topic of failed message kafka Topic
 --topicmap=<map_in_json> topic map in JSON format, like: {"a_*.log":"a_topic", "b_*.log":"b_topic"}
 --brokerlist=<broker> broker list, like: "192.168.1.10:9092,192.168.1.11:9092"
 --kafkabuffer=<size>  kafaka client buffer size, system default is 256
 --starttime=<stime>   set collect starttime[default: 1h]
`

var mainRetryer *Retryer
var remoteAvailable bool = true

func init() {
	formatter := log.TextFormatter{}
	formatter.DisableTimestamp = false
	formatter.DisableColors = true
	log.SetFormatter(&formatter)
	//log.SetLevel(log.InfoLevel)
}

func main() {
	args, err := docopt.Parse(usage, nil, true, "logflume v1.0", false)
	if err != nil {
		log.Error("Parse command line error: ", err)
	}
	log.Println(args)
	if args["--daemon"] != nil {
		daemon_mode = args["--daemon"].(bool)
	}
	if args["--cpus"] != nil {
		cpusStr := args["--cpus"].(string)
		cpus, _ = strconv.Atoi(cpusStr)
	}

	if args["--log-level"] != nil {
		levelStr := args["--log-level"].(string)
		level, err := log.ParseLevel(levelStr)
		if err != nil {
			log.Errorf("Parse level string(%s) failed, error: %s", levelStr, err)
		} else {
			log.SetLevel(level)
		}
	}

	if args["--work-dir"] != nil {
		work_dir = args["--work-dir"].(string)
	}

	if args["--checkpoint"] != nil {
		checkpoint = args["--checkpoint"].(string)
	}

	if args["--tail"] != nil {
		tailOnLog = args["--tail"].(bool)
	}

	if args["--deadtime"] != nil {
		deadtime = args["--deadtime"].(string)
	}

	if args["--topic"] != nil {
		kafkaTopic = args["--topic"].(string)
		retryTopic = kafkaTopic
	}

	if args["--retrytopic"] != nil {
		retryTopic = args["--retrytopic"].(string)
	}

	if args["--topicmap"] != nil {
		topicmapStr := args["--topicmap"].(string)
		if err := json.Unmarshal([]byte(topicmapStr), &topicmap); err != nil {
			log.Error(err)
			os.Exit(2)
		}
	}

	if args["--brokerlist"] != nil {
		brokerListStr := args["--brokerlist"].(string)
		brokerList = strings.Split(brokerListStr, ",")
	}

	if args["--kafkabuffer"] != nil {
		kafkabufferStr := args["--kafkabuffer"].(string)
		kafkabuffer, _ = strconv.Atoi(kafkabufferStr)
	}

	if args["--starttime"] != nil {
		starttime = args["--starttime"].(string)
	}

	log.Println(daemon_mode, cpus, work_dir, checkpoint, tailOnLog, deadtime, kafkaTopic, retryTopic, brokerList)
	log.Println(topicmap)

	if cpus < 2 {
		cpus = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(cpus)

	done := make(chan bool)
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Debug(sig)
		close(done)
	}()

	if err := os.Chdir(work_dir); err != nil {
		log.Fatalln("change dir error:", err)
	}
	if _, err := os.Stat(".lock"); os.IsNotExist(err) {
		log.Debug("starting logflume")
	} else {
		log.Error("already run")
		os.Exit(2)
	}
	lockFile, err := os.OpenFile(".lock", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("create pid file failed! error:", err)
	}

	defer lockFile.Close()
	defer func() {
		log.Info("remove .lock file")
		os.Remove(".lock")
	}()

	if _, err := lockFile.WriteString(fmt.Sprintf("%d", os.Getpid())); err != nil {
		log.Error("write lock file failed, error:", err)
	}

	mainRetryer, err = NewRetryer("retry/backup/retry.list")
	if err != nil {
		log.Error("Create mainRetryer failed, error:", err)
		return
	}

	prospector := Prospector{checkpoint: checkpoint, files: make(map[string]*FileState)}
	go prospector.Prospect(done)
	go mainRetryer.doRetry()

	<-done
}

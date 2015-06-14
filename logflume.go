package main

import (
	"fmt"
	"github.com/docopt/docopt-go"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

var checkpoint string
var (
	cpus              = 2
	daemon_mode       = false
	work_dir          = "./logflume"
	tailOnLog         = false
	harvestBufferSize = 16 << 10 //16k
	deadtime          = "1h"     //1 hour
	defaultTopic      = "test"   //kafka Topic
	defaultBrokerList = []string{"127.0.0.1:9092"}
	kafkabuffer       = 256
)

var kafkaTopic = defaultTopic
var retryTopic = kafkaTopic
var hashKey = "test-key"
var brokerList = defaultBrokerList

var usage = `usage: logflume (--checkpoint=<path> | -c=<path>) [options]

Options:
 -h, --help    Help info
 -d, --daemon    Daemon mode[default: false]
 -w, --work-dir=<path>    Work directory[default: logflume]
 -c, --checkpoint=<path>    Check point, directory or an file, terminal with / means a directory
 -k, --cpus=<num>    Num of CPU logflume use
 -t, --tail=<flag>    Tail on Log
 --deadtime=<time>    The time between last modify, set log to dead[default: 1h](m:Minute, h:Hour)
 --topic=<topic>	kafka Topic
 --retrytopic=<retry> retry Topic of failed message kafka Topic
 --brokerlist=<broker> broker list, like: "192.168.1.10:9092,192.168.1.11:9092"
 --kafkabuffer=<size>  kafaka client buffer size, system default is 256
`

var mainRetryer *Retryer
var remoteAvailable bool = true

func main() {
	args, err := docopt.Parse(usage, nil, true, "logflume v1.0", false)
	if err != nil {
		log.Println("Parse command line error: ", err)
	}
	fmt.Println(args)
	if args["--daemon"] != nil {
		daemon_mode = args["--daemon"].(bool)
	}
	if args["--cpus"] != nil {
		cpusStr := args["--cpus"].(string)
		cpus, _ = strconv.Atoi(cpusStr)
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

	if args["--brokerlist"] != nil {
		brokerListStr := args["--brokerlist"].(string)
		brokerList = strings.Split(brokerListStr, ",")
	}

	if args["--kafkabuffer"] != nil {
		kafkabufferStr := args["--kafkabuffer"].(string)
		kafkabuffer, _ = strconv.Atoi(kafkabufferStr)
	}

	fmt.Println(daemon_mode, cpus, work_dir, checkpoint, tailOnLog, deadtime, kafkaTopic, retryTopic, brokerList)

	if cpus < 2 {
		cpus = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(cpus)

	done := make(chan bool)
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Println(sig)
		close(done)
	}()

	if work_dir[0] == '/' {
		if err := os.Chdir(work_dir); err != nil {
			log.Fatalln("change dir error:", err)
		}
	}
	if _, err := os.Stat(".lock"); os.IsNotExist(err) {
		log.Println("starting logflume")
	} else {
		log.Println("already run")
		os.Exit(2)
	}
	lockFile, err := os.OpenFile(".lock", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("create pid file failed! error:", err)
	}

	defer lockFile.Close()
	defer func() {
		log.Println("remove .lock file")
		os.Remove(".lock")
	}()

	if _, err := lockFile.WriteString(fmt.Sprintf("%d", os.Getpid())); err != nil {
		log.Println("write lock file failed, error:", err)
	}

	mainRetryer, err = NewRetryer("retry/backup/retry.list")
	if err != nil {
		log.Println("Create mainRetryer failed, error:", err)
		return
	}

	prospector := Prospector{checkpoint: checkpoint, files: make(map[string]*FileState)}
	go prospector.Prospect(done)
	go mainRetryer.doRetry()

	<-done
}

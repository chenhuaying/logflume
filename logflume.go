package main

import (
	"fmt"
	"github.com/docopt/docopt-go"
	"log"
	"runtime"
)

var checkpoint string
var (
	cpus              = 2
	daemon_mode       = false
	work_dir          = "./logflume"
	tailOnLog         = false
	harvestBufferSize = 16 << 10 //16k
	deadtime          = "1m"     //1 hour
)

var usage = `usage: logflume (--checkpoint=<path> | -c=<path>) [options]

Options:
 -h, --help    Help info
 -d, --daemon    Daemon mode[default: false]
 -w, --work-dir=<path>    Work directory[default: logflume]
 -c, --checkpoint=<path>    Check point, directory or an file, terminal with / means a directory
 -k, --cpus=<num>    Num of CPU logflume use
 -t, --tail=<flag>    Tail on Log
`

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
		cpus = args["--cpus"].(int)
	}

	if args["--checkpoint"] != nil {
		checkpoint = args["--checkpoint"].(string)
	}

	runtime.GOMAXPROCS(cpus)

	done := make(chan bool)
	eventChan := make(chan *FileEvent, 16)
	registrarChan := make(chan *FileEvent, 16)

	prospector := Prospector{checkpoint: checkpoint, files: make(map[string]*FileState)}
	go prospector.Prospect(done, eventChan)
	go Publish(eventChan, registrarChan)
	Registrar(registrarChan)
}

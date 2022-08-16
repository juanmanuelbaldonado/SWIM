package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"swim/swim"
	"time"

	"github.com/mkideal/cli"
)

type stringDecoder struct {
	list []string
}

// Decode implements cli.Decoder interface
func (d *stringDecoder) Decode(s string) error {
	d.list = strings.Split(s, ",")
	return nil
}

type arguments struct {
	cli.Helper
	Host           string        `cli:"a,host" usage:"Host" dft:"127.0.0.1"`
	Port           int           `cli:"p,port" usage:"Port" dft:"10000"`
	Lambda         float64       `cli:"lambda" usage:"Dissemination buffere lambda" dft:"20.0"`
	Period         int64         `cli:"period" usage:"Failure detector ping period in millis" dft:"100"`
	GroupSize      int64         `cli:"group" usage:"Failure detector group size" dft:"4"`
	Timeout        int64         `cli:"timeout" usage:"Failure detector timeout in millis" dft:"50"`
	BootstrapNodes stringDecoder `cli:"bootstrap" usage:"Bootstrap nodes"`
}

func main() {
	os.Exit(cli.Run(new(arguments), func(ctx *cli.Context) error {
		args := ctx.Argv().(*arguments)
		config := swim.Config{
			Host: args.Host,
			Port: args.Port,
			FailureDetectorConfig: swim.FailureDetectorConfig{
				Period:    time.Duration(args.Period) * time.Millisecond,
				GroupSize: int32(args.GroupSize),
				Timeout:   time.Duration(args.Timeout) * time.Millisecond,
			},
			DisseminationBufferConfig: swim.DisseminationBufferConfig{
				Lambda: args.Lambda,
			},
			BootstrapNodes: make([]net.UDPAddr, 0),
		}
		for _, node := range args.BootstrapNodes.list {
			host := strings.Split(node, ":")[0]
			port, _ := strconv.Atoi(strings.Split(node, ":")[1])
			addr := net.UDPAddr{IP: net.ParseIP(host), Port: port}
			config.BootstrapNodes = append(config.BootstrapNodes, addr)
		}
		fmt.Println(config)
		member, err := swim.NewSWIMNode(config)
		if err != nil {
			panic(err)
		}
		go member.Start()
		time.Sleep(3 * time.Second)
		member.Probe()
		time.Sleep(10 * time.Second)
		return nil
	}))
}

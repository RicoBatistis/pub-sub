package main

import "flag"

var (
	topicID = flag.String("topic", "trial-L", "Topic name for publishing")
)

func main() {
	flag.Parse()

}

package wrpc

import(
	"strings"
	"strconv"
)

type Node struct {
	address string
	port int
	weight int
}

func ServerNode(node string) *Node {
	split := strings.Split(node, ":")
	port,_ := strconv.Atoi(split[1])
	if len(split) >= 3 {
		weight,_ := strconv.Atoi(split[2])
		return &Node{split[0], port, weight}
	}
	return &Node{split[0], port, WEIGHT_DEFAULT}
}

func (n *Node) GetAddress() string {
	return n.address
}

func (n *Node) GetPort() int {
	return n.port
}

func (n *Node) GetWeight() int {
	return n.weight
}

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type Node struct {
	Name       string
	IP         string
	Port       string
	Connection net.Conn
}

type Transaction struct {
	TransactionID int
	Transaction   string
	Sender        string
	Priority      int
	Deliverable   bool
	MessageType   string
}

var numNodes int
var parsedConfiguration [][]string // Each element is one line of the configuration file, split by spaces
var nodes []Node
var CONFIG_PATH string
var CURRENT_NODE string

var transactionChannel = make(chan Transaction)

/*
Parse the configuration file and store the contents in parsedConfiguration
*/
func parseConfigurationFile() {
	file, err := os.Open(CONFIG_PATH)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Advance the scanner to the first line and save it as the number of nodes
	if scanner.Scan() {
		numNodes, err := strconv.Atoi(scanner.Text())
		fmt.Println("Number of nodes: ", numNodes)
		if err != nil {
			fmt.Println("Error converting number of nodes: ", err)
			return
		}
	} else {
		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading first line: ", err)
		} else {
			fmt.Println("File is empty")
		}
		return
	}

	// Process the rest of the file
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		parsedConfiguration = append(parsedConfiguration, parts)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading configuration file: ", err)
		return
	}

}

/*
Check if a connection to a node is nodes already exists
*/
func connectionExists(name, ip, port string) bool {
	for _, node := range nodes {
		if node.IP == ip && node.Port == port && node.Name == name {
			return true
		}
	}
	return false
}

/*
Establishes connections with all nodes in the configuration file
*/

func handleConfiguration() {
	// Iterate over the parsedConfiguration
	for _, config := range parsedConfiguration {
		if len(config) != 3 {
			fmt.Println("Invalid configuration:", config)
			continue
		}

		nodeName := config[0]
		nodeIP := config[1]
		nodePort := config[2]

		// Skip if the node is the current node
		if nodeName == CURRENT_NODE {
			fmt.Println("Skipping self node:", nodeName)
			continue
		}

		// Check if a connection to the node already exists
		if !connectionExists(nodeName, nodeIP, nodePort) {
			conn, err := net.Dial("tcp", nodeIP+":"+nodePort)
			if err != nil {
				fmt.Println("Error connecting to node:", nodeName, err)
				continue // Use continue instead of return to try the next node
			}

			// Store the connection and save the node
			node := Node{
				Name:       nodeName,
				IP:         nodeIP,
				Port:       nodePort,
				Connection: conn,
			}
			nodes = append(nodes, node)

			fmt.Println("Successfully connected to node:", nodeName)
		} else {
			fmt.Println("Connection already exists for node:", nodeName)
		}
	}
	fmt.Println("Node configuration completed! These are the currently connected nodes:", nodes)

	// Goroutine to listen for transaction data from the channel and send it over the network
	go func() {
		for {
			transaction := <-transactionChannel
			// Serialize the transaction object into a byte stream
			transactionBytes, err := json.Marshal(transaction)
			if err != nil {
				fmt.Println("Error serializing transaction:", err)
				continue
			}

			// Send the serialized transaction over the network to each connected node
			for _, node := range nodes {
				_, err := node.Connection.Write(transactionBytes)
				if err != nil {
					fmt.Println("Error sending transaction to", node.Name, ":", err)
				}
			}
		}
	}()
}

func generateTransactions() {
	// Counter for transaction ID and priority
	var transactionID int
	var priority int

	for {
		// Generate a random transaction
		transactionID++
		priority++
		transaction := Transaction{
			TransactionID: transactionID,
			Transaction:   "Sample transaction",
			Sender:        CURRENT_NODE,
			Priority:      priority,
			Deliverable:   true,
			MessageType:   "message",
		}

		fmt.Println("This is working!")
		// Serialize the transaction object into a byte stream
		transactionBytes, err := json.Marshal(transaction)
		if err != nil {
			fmt.Println("Error serializing transaction:", err)
			continue
		}

		// Send the serialized transaction over the network to each connected node
		for _, node := range nodes {
			_, err := node.Connection.Write(transactionBytes)
			if err != nil {
				fmt.Println("Error sending transaction to", node.Name, ":", err)
			}
		}
	}
}

func main() {

	/*
		Establish node connection to port
	*/

	arguments := os.Args
	if len(arguments) < 2 {
		fmt.Println("Incorrect number of arguments")
		return
	}

	CURRENT_NODE = arguments[1]
	CONFIG_PATH = arguments[2]
	var PORT string

	parseConfigurationFile()

	// Compares CURRENT_NODE to the confirguation file to find which port to listen on
	for line := range parsedConfiguration {
		if parsedConfiguration[line][0] == CURRENT_NODE { // If found, add self-connection to nodes list
			PORT = ":" + parsedConfiguration[line][2]
			var node = Node{
				Name: parsedConfiguration[line][0],
				IP:   parsedConfiguration[line][1],
				Port: PORT[1:],
			}
			nodes = append(nodes, node)
		}
	}

	if PORT == "" {
		fmt.Println("Node not found in configuration file")
	}

	// Establish listener to specified port
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Server listening on port: ", PORT[1:])
	defer l.Close()

	// Handler for incoming connections. If a connection is established, add it to the nodes list
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer conn.Close()

			remoteIP := conn.RemoteAddr().(*net.TCPAddr).IP.String()
			fmt.Println("New connection from", remoteIP)

			// Check if the IP matches any in the parsedConfiguration and append if so
			for _, config := range parsedConfiguration {
				nodeName := config[0]
				nodeIP := config[1]
				nodePort := config[2]

				// Check if the remote IP matches the node IP in the configuration
				if remoteIP == nodeIP {
					fmt.Println("Matching IP found in configuration:", remoteIP)

					// Ensure no duplicate connections
					if !connectionExists(nodeName, nodeIP, nodePort) {
						node := Node{
							Name:       nodeName,
							IP:         nodeIP,
							Port:       nodePort,
							Connection: conn,
						}
						nodes = append(nodes, node)
						fmt.Printf("Added node %s to nodes list\n", nodeName)
					} else {
						fmt.Printf("Connection already exists for node %s\n", nodeName)
					}

					// Found a matching IP, no need to check further
					break
				}
			}
		}
	}()

	configTrigger := make(chan bool)
	evalTrigger := make(chan bool)

	// Handler for reading user input. If "CONFIG" is entered, trigger the configuration handler. If "EVAL" is entered, print the nodes list
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			text, _ := reader.ReadString('\n')
			switch strings.TrimSpace(text) {
			case "CONFIG":
				configTrigger <- true
				// fmt.Print("configTrigger sent\n")
			case "EVAL":
				evalTrigger <- true
			case "START":
				// Run the Python command to start generating transactions
				cmd := exec.Command("python3", "-u", "gentx.py", "0.5", "|", "./mp1_node", "node1", "config.txt")
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				err := cmd.Run()
				if err != nil {
					fmt.Println("Error running command:", err)
				}
			}
		}
	}()

	for {
		select {
		case <-configTrigger:
			go handleConfiguration()
		case <-evalTrigger:
			if len(nodes) == 0 {
				fmt.Println("No nodes to evaluate")
			} else {
				for _, node := range nodes {
					fmt.Println(node.Name, node.IP, node.Port)
				}
			}
		}
	}

	return
}

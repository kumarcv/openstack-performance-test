package main
import (
    "fmt"
    "os"
    "io/ioutil"
    "encoding/json"
    "net/http"
    "bytes"
    "time"
    "strings"
    "github.com/streadway/amqp"
    "log"
    //"net/url"
)

type ServerMsg struct {
    _context_request_id   string
    Context_request_id   string
    Priority              string
}

type PGConfig struct {
    ConfigName     string
    Type           string
    ConnectionRate int
    Connections    int
    Bombard        bool
    NovaUrl        string
    Authinfo       AuthInfo
    Rmquser        string
    Rmqpass        string
}

type AuthInfo struct {
    UserName     string
    Password      string
    TenantName   string
    AuthUrl      string
}

type ServerResp struct {
    Server	ServerInfo
}

type ServerInfo struct {
    FlavorRef     string
    ImageRef      string
    Name          string
    Id            string
    Links         []ServerLink
    AdminPass     string
}

type ServerLink struct {
    Href          string
    Rel           string
}

type Auth struct {
        Access Access
}

type Access struct {
        Token          Token
        User           User
        ServiceCatalog []Service
}

type Token struct {
        Id      string
        Expires time.Time
        Tenant  Tenant
}
type Tenant struct {
        Id   string
        Name string
}

type User struct {
        Id          string
        Name        string
        Roles       []Role
        Roles_links []string
}

type Role struct {
        Id       string
        Name     string
        TenantId string
}

type Service struct {
        Name            string
        Type            string
        Endpoints       []Endpoint
        Endpoints_links []string
}

type Endpoint struct {
        TenantId    string
        PublicURL   string
        InternalURL string
        Region      string
        VersionId   string
        VersionInfo string
        VersionList string
}


//Globals
var authInfo Auth
var pgconfig PGConfig
type Data struct {
    RequestId     string
    TestId        string    
    Responsetime  time.Duration
}
type Consumer struct {
        conn    *amqp.Connection
        channel *amqp.Channel
        tag     string
        done    chan error
}


var AsyncRespChannel = make(chan Data)
var respTime = make(map[string]map[string]time.Duration)

var dataChannel = make(chan Data)
var rmqchannel = make(chan PGConfig)
func (c *Consumer) Shutdown() error {
        // will close() the deliveries channel
        if err := c.channel.Cancel(c.tag, true); err != nil {
                return fmt.Errorf("Consumer cancel failed: %s", err)
        }

        if err := c.conn.Close(); err != nil {
                return fmt.Errorf("AMQP connection close error: %s", err)
        }

        defer log.Printf("AMQP shutdown OK")

        // wait for handle() to exit
        return <-c.done
}
func handle(deliveries <-chan amqp.Delivery, done chan error) {
        fmt.Printf("Handling AMQP messages %v\n", deliveries)
        for d := range deliveries {
                fmt.Printf("rcvd...")
                //log.Printf(
                //        "got %dB delivery: [%v] %q",
                //        len(d.Body),
                //        d.DeliveryTag,
                //        d.Body,
                //)
                var msg ServerMsg
                json.Unmarshal(d.Body, &msg)
                fmt.Printf("msg is %v\n",msg)
                /* Ack the previous messages as well */
                d.Ack(true)
        }
        log.Printf("handle: deliveries channel closed")
        done <- nil
}

func loop() {
    /* We defined channel for each request go routines to synchronize
     * and send results.
     */ 
    fmt.Printf("loop\n")
    c, err := NewConsumer("amqp://guest:ravi@localhost:5672", "nova", "fanout", "perf-tool", "notifications.info", "perf-tool")
    if err != nil {
        log.Fatalf("%s", err)
    }

    for {
        select {
            case data := <- AsyncRespChannel:
                // Collect the response time and store in a map 
                fmt.Printf("Recevied from channel %v\n", data)
                reqd, ok := respTime[data.TestId]
                if !ok {
                    reqd = make(map[string]time.Duration)
                    reqd[data.RequestId] = data.Responsetime
                    respTime[data.TestId] = reqd
                } else {
                    respTime[data.TestId][data.RequestId] = data.Responsetime
                }
                fmt.Printf("respTime map is %v\n", respTime)
           // case pgconfig := <- rmqchannel:
                //init_consumer(pgconfig.Rmquser, pgconfig.Rmqpass)
        }
    }
    log.Printf("shutting down")

    if err := c.Shutdown(); err != nil {
        log.Fatalf("error during shutdown: %s", err)
    }
 
}


func main() {
    // Handle web services in a go routine
    go func() {
        http.HandleFunc("/server", server_handler)
        http.HandleFunc("/config", config_handler)
        http.HandleFunc("/server/result", server_result_handler)
        http.ListenAndServe(":8080", nil)
    }()

    loop()

}

func generate_uuid() string {
    f, _ := os.Open("/dev/urandom")
    b := make([]byte, 16)
    f.Read(b)
    f.Close()
    return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func server_result_handler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprintf(w, "%v", respTime)
}

func validate_content(contentType []string) bool {
    var isContentJson bool
    fmt.Printf("validate content\n")
    for _,value := range contentType {
        if value == "application/json" { 
           isContentJson = true 
        }
    }
    fmt.Printf("returning from validate content\n")
    return isContentJson
}

func set_content_type(w http.ResponseWriter, content string) {
    w.Header().Set("Content-Type", content)
}

func server_handler(w http.ResponseWriter, r *http.Request) {
    fmt.Printf("Request is %v\n", r)
    switch r.Method {
        case "GET":
            //Get the result of a test. Make sure server id is passed 
            fmt.Printf("URL is %v\n", r.URL)
            // For now send URL back to the caller 
            fmt.Fprintf(w, "%v", r.URL)
        case "POST":
            body, err := ioutil.ReadAll(r.Body)
            if err != nil {
                fmt.Fprintf(w, "Wrong Content sent %v", err)
            }
            post_server(w, r, body)
        default:
            fmt.Fprintf(w, "only GET and POST supported")
    } 
   
} 
func post_server(w http.ResponseWriter, r *http.Request, body []byte) {    
    fmt.Printf("\n Processing Server POST\n")
    contentType := r.Header["Content-Type"]
    isContentJson := validate_content(contentType)
    if isContentJson == false {
        fmt.Fprintf(w, "Hi, the following Content types are not supported %v\n", contentType)
        fmt.Printf("invalid content type\n")
    } else {
        var server ServerInfo
        err := json.Unmarshal(body, &server)
        if err != nil {
            fmt.Fprintf(w, "Couldnot decode the body sent %v", err)
        }
        fmt.Printf("body got is %v\n", server)
        uuid := generate_uuid()
        go launchservers(server, w, uuid)
        fmt.Fprintf(w, "Get results by UUID: %v", uuid)
    }
}


func config_handler(w http.ResponseWriter, r *http.Request) {
    fmt.Printf("Request is %v\n", r)
    contentType := r.Header["Content-Type"]
    isContentJson := validate_content(contentType)
    if isContentJson == false {
        fmt.Fprintf(w, "Hi, the following Content types are not supported %v\n", contentType)
    } else {
        //fmt.Fprintf(w, "Hi There, I love %s!", r.URL.Path[1:])
        body, err := ioutil.ReadAll(r.Body)
        if err != nil {
            fmt.Fprintf(w, "Wrong Content sent")
        }
        err = json.Unmarshal(body, &pgconfig)
        fmt.Printf("body got is %v\n", pgconfig)
        fmt.Fprintf(w, "Received the following config %v\n", pgconfig) 
        if pgconfig.Authinfo != (AuthInfo{}) {
          go gettoken(pgconfig.Authinfo)
        }
        //rmqchannel <- pgconfig
    }
}

func gettoken(auth AuthInfo) {
    fmt.Printf("Sending a Keystone Request\n")
    authstr := (fmt.Sprintf(`{"auth":{
                "passwordCredentials":{"username":"%s","password":"%s"},"tenantName":"%s"}
                }`,
                auth.UserName, auth.Password, auth.TenantName))
 
    resp, err := http.Post((auth.AuthUrl+ "/" + "tokens"), "application/json", bytes.NewBufferString(authstr))
    if err != nil {
        fmt.Printf("%v\n", err)
    }
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        fmt.Printf("Error in reading response body: %v\n", err)
        os.Exit(1) 
    }
    if err = json.Unmarshal(body, &authInfo); err != nil {
       fmt.Printf("Error in parsing body: %v\n", err)         
    }
    fmt.Printf("Token is %v\n", authInfo.Access.Token.Id)
}

func launchservers(server ServerInfo, w http.ResponseWriter, uuid string) {
    fmt.Printf("Launching servers\n")
    serverStr := (fmt.Sprintf(`{"server":{"flavorRef":"%s", "imageRef":"%s", "name":"%s"} }`,
                 server.FlavorRef, server.ImageRef,server.Name))
    for i:= 0; i< pgconfig.Connections; i++ {
        go test(serverStr, w, uuid)
    }
}

func test(serverStr string, w http.ResponseWriter, uuid string) {
        client := &http.Client{}
        buf := strings.NewReader(serverStr)
        req, err := http.NewRequest("POST", pgconfig.NovaUrl+"/"+ authInfo.Access.Token.Tenant.Id+ "/" + "servers", buf)
        if err != nil {
            fmt.Printf("%v\n", err)
        }
        
        req.Header.Add("Content-Type", "application/json")
        req.Header.Add("Accept", "application/json")
        req.Header.Add("X-Auth-Token", authInfo.Access.Token.Id)
        req.Header.Add("X-Auth-Project-Id", authInfo.Access.Token.Tenant.Name)
        fmt.Printf("request is %v\n", req)
        t0 := time.Now()
        resp, err := client.Do(req)
        if err != nil {
            fmt.Printf("%v\n", err)
        }
        t1 := time.Since(t0)
        reqid := resp.Header.Get("X-Compute-Request-Id")
  
        // parse the body and get the server id
        var server ServerResp
        body, err := ioutil.ReadAll(resp.Body)
        if err != nil {
            fmt.Printf("Error in reading response body: %v\n", err)
            os.Exit(1) 
        }
        
        if err = json.Unmarshal(body, &server); err != nil {
           fmt.Printf("Error in parsing body: %v\n", err)         
        }
        fmt.Printf("body is %v\n", string(body))
        fmt.Printf("Server response is %v\n", server)
        for _, link := range server.Server.Links {
            href := link.Href
            fmt.Printf("href is %v\n", href)
        }
        fmt.Printf("Response took %v and status is %v for server id %v\n", t1, resp.Status, reqid)
        var data Data
        data.TestId = uuid
        data.RequestId = reqid
        data.Responsetime = t1
        AsyncRespChannel <- data
}

func NewConsumer(amqpURI, exchange, exchangeType, queueName, key, ctag string) (*Consumer, error) {
        c := &Consumer{
                conn:    nil,
                channel: nil,
                tag:     ctag,
                done:    make(chan error),
        }

        var err error

        log.Printf("dialing %q", amqpURI)
        c.conn, err = amqp.Dial(amqpURI)
        if err != nil {
                return nil, fmt.Errorf("Dial: %s", err)
        }

        go func() {
                fmt.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
        }()

        log.Printf("got Connection, getting Channel")
        c.channel, err = c.conn.Channel()
        if err != nil {
                return nil, fmt.Errorf("Channel: %s", err)
        }

        log.Printf("got Channel, declaring Exchange (%q)", exchange)
/*
        if err = c.channel.ExchangeDeclare(
                exchange,     // name of the exchange
                exchangeType, // type
                true,         // durable
                false,        // delete when complete
                false,        // internal
                false,        // noWait
                nil,          // arguments
        ); err != nil {
                return nil, fmt.Errorf("Exchange Declare: %s", err)
        }
*/
        log.Printf("declared Exchange, declaring Queue %q", queueName)
        queue, err := c.channel.QueueDeclare(
                queueName, // name of the queue
                true,      // durable
                false,     // delete when usused
                false,     // exclusive
                false,     // noWait
                nil,       // arguments
        )
        if err != nil {
                return nil, fmt.Errorf("Queue Declare: %s", err)
        }
        log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
                queue.Name, queue.Messages, queue.Consumers, key)
        if err = c.channel.QueueBind(
                queueName, // name of the queue
                key,        // bindingKey
                exchange,   // sourceExchange
                false,      // noWait
                nil,        // arguments
        ); err != nil {
                return nil, fmt.Errorf("Queue Bind: %s", err)
        }
        log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
/*
        if err = c.channel.ExchangeBind(
                exchange,     // name of the exchange
                key, // type
                "neutron",         // durable
                false,        // noWait
                nil,          // arguments
        ); err != nil {
                return nil, fmt.Errorf("Exchange Bind: %s", err)
        }
*/
        log.Printf("Exchange Binded to neutron, starting Consume (consumer tag %q)", c.tag)


        deliveries, err := c.channel.Consume(
                queueName, // name
                c.tag,      // consumerTag,
                false,      // noAck
                false,      // exclusive
                false,      // noLocal
                false,      // noWait
                nil,        // arguments
        )
        if err != nil {
                return nil, fmt.Errorf("Queue Consume: %s", err)
        }

        go handle(deliveries, c.done)

        return c, nil
}



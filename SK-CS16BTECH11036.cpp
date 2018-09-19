#include <bits/stdc++.h>
#include <string>   //strlen 
#include <errno.h> 
#include <unistd.h>   //close 
#include <arpa/inet.h>    //close 
#include <sys/types.h> 
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <sys/time.h> //FD_SET, FD_ISSET, FD_ZERO macros 
#include <thread>
#include <mutex>
#include <atomic>

using namespace std;

int n,e,m;

float a;

mutex mtx;

int PORT = 5000;

//vector for graph topology
vector<int>* graph;

//atomic variables for checking correctness
atomic<int> connections {0};
atomic<int> server_on {0};
atomic<int> completion {0};
atomic<int> pending_messages{0};
atomic<int> tupel_received{0};

//output files
fstream output_SK;
fstream out_compare;

time_t curr_time;
tm* local_time;

//variable for maximum connections in graph
int max_connections = 0;

//function which gives value for exponential decay
double random_exponential_decay(float lambda)
{
    default_random_engine generate;
    exponential_distribution<double> distribution(1.0/lambda);
    return distribution(generate);
}

/* 
This functon is called to parse the input message from a client.
It may happen that may send message may have been sent to this server 
so it parses accordingly.
*/
void parser_comp(string s, int vec_clock[], int last_update[],
                     int last_send[], int port_id)
{
    //storing new vector clock
    int new_vec_clock[n];
    string temp = "";
    int index = 0;
    string sender = "";
    string indexer = "";
    int switcher;
    for(int j = 0; j < s.length(); j++)
    {
        if(s[j] == ':')
        {
            sender = "";
            temp = "";
            indexer = "";
            switcher = 0;
            vec_clock[port_id]++;
            last_update[port_id]++;
        }
        else if(s[j] == ';')
        {
            assert(s[j+1] == '(');

        }
        else if(s[j] == '(')
        {
            switcher = 1;
        }
        else if(s[j] == ',')
        {
            index = stoi(indexer);
            switcher = 2;
            indexer = "";
        }
        else if(s[j] == ')')
        {
            assert(s[j+1] == '|' || s[j+1] == '(');
            if(vec_clock[index] < stoi(temp))
                vec_clock[index] = stoi(temp);
            last_update[index] = vec_clock[port_id];
            temp = "";
            
        }
        else if(s[j] == '|')
        {
            assert(s[j+1] == ':' || s[j+1] == '\0');
            pending_messages--;
            mtx.lock();
            curr_time = time(0);
            local_time = localtime(&curr_time);
            output_SK << "Process" << port_id + 1 << " receives message m" << stoi(sender) + 1 << "_"
            << vec_clock[stoi(sender)] << " from Process" << stoi(sender) + 1
            << " at " << local_time->tm_min << ":" << local_time->tm_sec << " vc: [ ";
            for(int i = 0; i < n; i++)
               output_SK << vec_clock[i] << " ";
            output_SK << "]" << endl;
            mtx.unlock();

        }
        else
        {
            if(switcher == 0)
                sender += s[j];
            else if(switcher == 1)
                indexer += s[j];
            else
                temp += s[j];
        }
    }
}

/*
This function runs the client part of each thread.
Here in this function the client chooses an internal event
or a message pass event according to the probability given
The message to send is selected at random using uniform probability distribution
*/

void clientRunner(int client_id, vector<int> conns, int vec_clock[], int last_update[], 
                   int last_send[], mutex* node_proc)
{
    //data structure for client_address of client
    struct sockaddr_in client_address;

    int string_length;

    //data structure for client_address of server
    struct sockaddr_in server_address;
    string msg;
    int port;
    char buffer[4096] = {0};
    string mssg;
    int max_conns = conns.size();
    int sock[max_conns] = {0};
    int to_send = 0;
    int flag = 0;
    int message_sent = 0;
    int internal_to_do = a*m;

    //this loop waits until all the servers are on and listening
    while(server_on < n);

    //this loop connects to all the servers according to the graph topology
    for(int i = 0; i < max_conns; i++)
    {
        if ((sock[i] = socket(AF_INET, SOCK_STREAM, 0)) < 0)    //creating sock_fd
            {
                printf("\n Socket creation error \n");
                return;
            }

        memset(&server_address, '0', sizeof(server_address));

        //setting server details
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(PORT + conns[i]);

        //checking the ip
        if(inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) <=0 ) 
        {
            printf("\nInvalid client_address %d \n", client_id);
            return;
        }

        //connecting to server
        if (connect(sock[i], (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
        {
            printf("\nConnection Failed for client %d \n", client_id);
            return;
        }

    }

    //waiting until all the connections are established
    while(connections < max_connections);

    //the loop for internal or message pass event
    while(completion < n && message_sent < m)
    {
        //selecting action according to probability
        bool action_internal = (rand()%100) < (a/(a+1))*100;

        //executing internal event
        if(action_internal && internal_to_do > 0)
        {
            node_proc->lock();
            vec_clock[client_id]++;
            last_update[client_id]++; 
            internal_to_do--;
            mtx.lock();
            curr_time = time(0);
            local_time = localtime(&curr_time);
            output_SK << "Process" << client_id + 1 << " executes internal event e" << client_id + 1 << "_"
            << vec_clock[client_id] << " at " << local_time->tm_min << ":" << local_time->tm_sec 
            <<  " vc: [ ";
            for(int i = 0; i < n; i++)
                output_SK << vec_clock[i] << " ";
            output_SK << "]" << endl;
            mtx.unlock();
            node_proc->unlock();
        }

        //executing message pass event
        else
        {
            //incrementing pending message
            pending_messages++;

            node_proc->lock();
            vec_clock[client_id]++;
            last_update[client_id]++;
            message_sent++;
            
            assert(last_update[client_id] == vec_clock[client_id]);
            //finding to whom to send
            to_send = rand()%max_conns;
            
            
            //creating message string
            mssg=":";
            mssg+=to_string(client_id)  + ";";
            mtx.lock();
            curr_time = time(0);
            local_time = localtime(&curr_time);
            output_SK << "Process" << client_id + 1 << " sends message m" << client_id + 1 << "_"
            << vec_clock[client_id] << " to process" << conns[to_send] + 1 << " at "
            << local_time->tm_min << ":" << local_time->tm_sec << " vc: [ ";

            for(int i = 0; i < n; i++)
            {
                if(last_send[to_send] < last_update[i])
                {
                    tupel_received++;
                    mssg += '(' + to_string(i) + "," + to_string(vec_clock[i]) + ')';
                }
                output_SK << vec_clock[i] << " ";
            }
            mssg += '|';
            output_SK << "]" << endl;
            mtx.unlock();

            
            last_send[to_send] = vec_clock[client_id];

            

            //sending the message
            if(send(sock[to_send], mssg.c_str(), mssg.length(), 0) != mssg.length())
            {
                perror("send");
            }
            node_proc->unlock();
        }

        //this sets the complete of program condition
        if(flag == 0 && message_sent >= m)
        {
            completion++;
            flag = 1;
        }

        //going to sleep
        this_thread::sleep_for(chrono::milliseconds((int)(random_exponential_decay(e))));
    }

    // waiting for other clients to complete
    while(completion < n || pending_messages > 0);

    //leaving and closing the client
    for(int i = 0; i < max_conns; i++)
        close(sock[i]);
}


/*
This function runs the server for each node. This function creates a 
array of clients which can be maximum n the number of nodes. Then it starts listening 
for multiple clients on its ip and port address. 
It has a function called select which selects the socket where activity happens.
When an activity happens it checks that whether it is new connection or IO and it handels 
accordingly.
*/
void run_server(int port_id, int vec_clock[], int last_update[], int last_send[], mutex* node_proc)
{

    //required variables
    int server_socket;
    int option = 1;
    int client_socket[n];
    int max_clients = n;
    int activity, i , string_length , curr_soc;
    int sever_address_length , new_socket, max_sd;
    int port = PORT + port_id;
    int sender, receiver;

    struct sockaddr_in sever_address;
    char buffer[1025];

    fd_set scoket_descriptor;

    string message;

    for (i = 0; i < max_clients; i++)  
    {  
        client_socket[i] = 0;  
    } 

    //creating the server socket
    if( (server_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0)  
    {  
        perror("socket creation failed");  
        exit(EXIT_FAILURE);  
    } 


    // making the server
    if( setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&option, 
          sizeof(option)) < 0 )  
    {  
        perror("setsockopt failed");  
        exit(EXIT_FAILURE);  
    }  

    //data structure for the server
    sever_address.sin_family = AF_INET;  
    sever_address.sin_addr.s_addr = INADDR_ANY;  
    sever_address.sin_port = htons( port );  

    //binding the server to listen to respective port
    if (bind(server_socket, (struct sockaddr *)&sever_address, sizeof(sever_address))<0)  
    {  
        perror("bind for socket failed");  
        exit(EXIT_FAILURE);  
    }  

    //lsiten to that socket and 5 is the waiting queue of clients
    if (listen(server_socket, 5) < 0)  
    {  
        perror("listen");  
        exit(EXIT_FAILURE);  
    }  

    sever_address_length = sizeof(sever_address);  
 
    server_on++;

    //run server loop
    while(completion < n || pending_messages > 0)  
    {  
        //clearing the socket 
        FD_ZERO(&scoket_descriptor);  
    
        //setting the server socket
        FD_SET(server_socket, &scoket_descriptor);  
        max_sd = server_socket;  
            
        //for loop for the client connected
        for ( i = 0 ; i < max_clients ; i++)  
        {  
            //socket for each client
            curr_soc = client_socket[i];  
                
            //if the scoket is readable than add to read list
            if(curr_soc > 0)  
                FD_SET( curr_soc , &scoket_descriptor);  
                
            //we need highest number of fd for select function
            if(curr_soc > max_sd)  
                max_sd = curr_soc;  
        }  
    
        /*
        waiting for activity on the socket,
        here the timeout is set to null so it waits infinetly
        The purpose of this method is to wake up the server if 
        something happens to its socket
        */
        activity = select(max_sd + 1 , &scoket_descriptor , NULL , NULL , NULL);  
      
        if ((activity < 0) && (errno!=EINTR))  
        {  
            printf("select error");  
        }  
            
        //checking new connection
        if (FD_ISSET(server_socket, &scoket_descriptor))  
        {  
            if ((new_socket = accept(server_socket, (struct sockaddr *)&sever_address, (socklen_t*)&sever_address_length))<0)  
            {  
                perror("accept");  
                exit(EXIT_FAILURE);  
            }  
            
            connections++;
                
            //adding new connection
            for (i = 0; i < max_clients; i++)  
            {  
                //adding to first non-empty position
                if( client_socket[i] == 0 )  
                {  
                    client_socket[i] = new_socket;      
                    break;  
                }  
            }  
        }  
            
        //checking each client for IO
        for (i = 0; i < max_clients; i++)  
        {  
            curr_soc = client_socket[i];  
                
            if (FD_ISSET( curr_soc , &scoket_descriptor))  
            { 
                //checking if some one disconnected
                if ((string_length = read( curr_soc , buffer, 4096)) == 0)  
                {  

                    getpeername(curr_soc , (struct sockaddr*)&sever_address ,(socklen_t*)&sever_address_length);  
                    close(curr_soc);  
                    client_socket[i] = 0; 
                }  
                    
                //receviving the message came in
                else
                {  

                    buffer[string_length] = '\0'; 
                    string please(buffer); 
                    node_proc->lock();
                    parser_comp(please, vec_clock, last_update, last_send, port_id);
                    node_proc->unlock();
                }  
            }  
        }  
    }  close(server_socket);
    //closing the server
    close(server_socket);

}

/*
This function creates the node of graph topology
*/
void node(int id)
{
    //vector of neighbours for current node
    vector<int> conns;

    mutex node_proc;

    for(int i = 0; i < graph[id].size(); i++)
        conns.push_back(graph[id][i]);

    //vector clock
    int* logical_time;
    int *last_update;
    int *last_send;

    logical_time = new int [n];
    last_update = new int [n];
    last_send = new int [n];

    for(int i = 0; i < n; i++)
    {
        logical_time[i] = 0;
        last_update[i] = 0;
        last_send[i] = 0;
    }
    
    thread threads[2];
    threads[0] = thread(run_server, id, logical_time, last_update, last_send, &node_proc);
    threads[1] = thread(clientRunner, id, conns, logical_time, last_update, last_send, &node_proc);

    for (auto &th : threads) th.join();

    return;
}

int main()
{
    srand(time(0));
    ifstream infile;
    output_SK.open("output-SK.txt", ios::out | ios::trunc);
    out_compare.open("comparison.txt", ios::app);
    infile.open("inp-params.txt");
    infile >> n >> e >> a >> m;
    graph = new vector<int> [n];
    int index;
    string input;
    int switcher;
    string temp;
    getline(infile, input);

    for(int i = 0; i < n; i++)
    {
        getline(infile, input);
        switcher = 0;
        temp = "";
        for(int j = 0; j < input.length(); j++)
        {
            if(input[j] == ' ' && switcher == 0)
            {
                switcher = 1;
                index = stoi(temp) - 1;
                temp = "";
            }
            else if(input[j] == ' ' && switcher == 1)
            {
                graph[index].push_back(stoi(temp) - 1);
                temp = "";
            }
            else 
            {
                temp += input[j];
            }
        }
        
        graph[index].push_back(stoi(temp) - 1);
    }

    thread main_threads[n];
    for(int i = 0; i < n; i++)
    {
        main_threads[i] = thread(node, i);
    }
    for (auto &th : main_threads) th.join();

     out_compare << "The input space used by Singhal-Kshemkalyani optimization is : O(" << 3*n << ")." << endl;
    out_compare << "The space used for sending message in Singhal-Kshemkalyani optimization, is : O(" << tupel_received << ")." << endl;
    return 0;
}


import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TreeMap;

import spread.AdvancedMessageListener;
import spread.MembershipInfo;
import spread.SpreadConnection;
import spread.SpreadException;
import spread.SpreadGroup;
import spread.SpreadMessage;

public class Server {
	
	// The Spread Connection.
	private SpreadConnection connection;
	
	// The keyboard input.
	private InputStreamReader inputKeyboard;

	// The group of the server.
	private SpreadGroup group;
	
	// The number of messages sent.
	private int numberOfMessagesSent;
	
	// The name of the server.
	private String serverName;
	
	// The priority of the server.
	private int priority;
	
	// The server name of the Master.
	private String master;
	
	// Structure to store the priority of all servers in the group.
	private Map<String, Integer> tmServers;
	
	public Server(String address, int port, String server, int priority) {
		
		if (Integer.parseInt(server.split("_")[1]) >= 0 &&
				Integer.parseInt(server.split("_")[1]) <= 1000) {
			
			// Setup the keyboard input.
			this.inputKeyboard = new InputStreamReader(System.in);
			
			// Establish the spread connection.
			try {
				
				this.connection = new SpreadConnection();
				this.connection.connect(InetAddress.getByName(address), port, server, false, true);
				
			} catch(SpreadException e) {
				
				System.err.println("There was an error connecting to the daemon.");
				
				e.printStackTrace();
				
				System.exit(1);
				
			} catch(UnknownHostException e) {
				
				System.err.println("Can't find the daemon " + address);
				
				e.printStackTrace();
				
				System.exit(1);
				
			}
			
			// Set the variables
			this.serverName = server;
			this.priority = priority;
			this.group = null;
			this.master = null;
			this.tmServers = new TreeMap<String, Integer>();
			
			
			// Add the listeners
			this.connection.add(new AdvancedMessageListener() {
				
				@Override
				public void regularMessageReceived(SpreadMessage message) {
					
					byte data[] = message.getData();
					String dataMessage = new String(data);
					
					// Message sent to a group
					if (Integer.parseInt(dataMessage.split("&")[1]) == 0) {
					
						SpreadMessage updatedMessage = message;
						updatedMessage.setData(new String(dataMessage.substring(3)).getBytes());
						
						displayRegularMessage(updatedMessage);
						
					}
					
					// Message to verify the Master
					else if (Integer.parseInt(dataMessage.split("&")[1]) == 1) {
						
						String splittedMessage[] = dataMessage.split("&");
						
						updateMaster(splittedMessage[2], splittedMessage[3], splittedMessage[4], Integer.parseInt(splittedMessage[5]));
						
					}
					
				}
				
				@Override
				public void membershipMessageReceived(SpreadMessage message) {

					displayMembershipMessage(message);
					
				}
				
			});
			
			// Show the menu.
			showMenu();
			
			// Get user command.
			while(true)
				getUserCommand();
			
		} else {
			
			System.err.println("Choose a server id between [0-1000].");
			
			System.exit(1);
			
		}
		
	}
	
	private void updateMaster(String instruction, String serverName, String priority, int groupSize) {
		
		if (instruction.equals("join")) {
			
			this.tmServers.put(serverName, Integer.parseInt(priority));
			
			if (groupSize == this.tmServers.size()) {
				
				String tempMaster = this.serverName;
				int tempPriority = this.priority;
				
				for (String key : this.tmServers.keySet()) {
					
					if (tmServers.get(key) >= tempPriority) {
						
						tempMaster = key;
						tempPriority = this.tmServers.get(key);
						
					}
					
				}
				
				this.master = tempMaster;
				
				System.out.println("\n" + this.serverName + " said that " + this.master + " is the Master.");
				
				System.out.println("\nALL SERVERS:");
				for (String key : this.tmServers.keySet())
					System.out.println("(" + key + "; PRIORITY: " + String.valueOf(this.tmServers.get(key)) + ")");
				
			}
			
		}
			
		else if (instruction.equals("leave") || instruction.equals("disconnect"))
			if (this.tmServers.containsKey(serverName)) {
				
				this.tmServers.remove(serverName);
				
				String tempMaster = this.serverName;
				int tempPriority = this.priority;
				
				for (String key : this.tmServers.keySet()) {
					
					if (tmServers.get(key) >= tempPriority) {
						
						tempMaster = key;
						tempPriority = this.tmServers.get(key);
						
					}
					
				}
				
				this.master = tempMaster;
				
				System.out.println("\n" + this.serverName + " said that " + this.master + " is the Master.");
				
				System.out.println("\nALL SERVERS:");
				for (String key : this.tmServers.keySet())
					System.out.println("(" + key + "; PRIORITY: " + String.valueOf(this.tmServers.get(key)) + ")");
				
			}
		
	}

	private void displayRegularMessage(SpreadMessage message) {

		try {
			
			System.out.println("\n**********LISTENER FOR REGULAR MESSAGES**********");
			
			if (message.isRegular()) {
				
				System.out.print("\nReceived a ");
				
				if (message.isUnreliable())
					System.out.print("UNRELIABLE");
				
				else if (message.isReliable())
					System.out.print("RELIABLE");
				
				else if (message.isFifo())
					System.out.print("FIFO");
				
				else if (message.isCausal())
					System.out.print("CAUSAL");
				
				else if(message.isAgreed())
					System.out.print("AGREED");
				
				else if(message.isSafe())
					System.out.print("SAFE");
				
				System.out.println(" message.");
				System.out.println("Sent by " + message.getSender());
				
				System.out.println("Type is " + message.getType() + ".");
				
				if(message.getEndianMismatch() == true)
					System.out.println("There is an endian mismatch.");
				else
					System.out.println("There is no endian mismatch.");

				SpreadGroup groups[] = message.getGroups();
				System.out.println("Sent to " + groups.length + " groups.");
				
				byte data[] = message.getData();
				System.out.println("The data has " + data.length + " bytes.");
				
				System.out.println("The message is: " + new String(data));
				
			}
			
		} catch(Exception e) {
			
			e.printStackTrace();
			
			System.exit(1);
			
		}
		
	}

	private void displayMembershipMessage(SpreadMessage message) {

		try {
			
			System.out.println("\n**********LISTENER FOR MEMBERSHIP MESSAGES**********");
			
			if (message.isMembership()) {
				
				MembershipInfo info = message.getMembershipInfo();
				
				if (info.isRegularMembership()) {
					
					SpreadGroup members[] = info.getMembers();
					MembershipInfo.VirtualSynchronySet virtual_synchrony_sets[] = info.getVirtualSynchronySets();
					MembershipInfo.VirtualSynchronySet my_virtual_synchrony_set = info.getMyVirtualSynchronySet();

					System.out.println("\nREGULAR membership for group " + group +
							   " with " + members.length + " members:");
					
					for (int i = 0; i < members.length; i++)
						System.out.println("\t" + members[i]);

					System.out.println("\nGroup ID is " + info.getGroupID());

					System.out.print("\nDue to ");
					
					if (info.isCausedByJoin()) {
						
						System.out.println("the JOIN of " + info.getJoined());
						
						SpreadMessage joinMessage = new SpreadMessage();
						joinMessage.setSafe();
						joinMessage.addGroup(info.getGroup().toString());
						joinMessage.setData(new String("&1&join&" + this.serverName + "&" + this.priority + "&" + String.valueOf(members.length)).getBytes());
						
						this.connection.multicast(joinMessage);
						
					}
					
					else if (info.isCausedByLeave()) {
						
						System.out.println("the LEAVE of " + info.getLeft());
						
						SpreadMessage leaveMessage = new SpreadMessage();
						leaveMessage.setSafe();
						leaveMessage.addGroup(info.getGroup().toString());
						leaveMessage.setData(new String("&1&leave&" + info.getLeft().toString().split("#")[1] + "&null&" + String.valueOf(members.length)).getBytes());
						
						this.connection.multicast(leaveMessage);
						
					}
					
					else if (info.isCausedByDisconnect()) {
						
						System.out.println("the DISCONNECT of " + info.getDisconnected());
					
						SpreadMessage disconnectMessage = new SpreadMessage();
						disconnectMessage.setSafe();
						disconnectMessage.addGroup(info.getGroup().toString());
						disconnectMessage.setData(new String("&1&disconnect&" + info.getDisconnected().toString().split("#")[1] + "&null&" + String.valueOf(members.length)).getBytes());
						
						this.connection.multicast(disconnectMessage);
						
					}
					
					else if (info.isCausedByNetwork()) {
						
						System.out.println("NETWORK change");
						
						for (int i = 0; i < virtual_synchrony_sets.length; i++ ) {
							
							MembershipInfo.VirtualSynchronySet set = virtual_synchrony_sets[i];
							SpreadGroup setMembers[] = set.getMembers();
							
							System.out.print("\t\t");
							
							if (set == my_virtual_synchrony_set)
								System.out.print("(LOCAL) ");
							else
								System.out.print("(OTHER) ");
							
							System.out.println("Virtual Synchrony Set " + i + " has " +
									    set.getSize() + " members:");
							
							for (int j = 0; j < set.getSize(); j++)
								System.out.println("\t\t\t" + setMembers[j]);
							
						}
						
					}
					
				} else if(info.isTransition())
					System.out.println("\nTRANSITIONAL membership for group " + group + ".");
				
				else if(info.isSelfLeave())
					System.out.println("\nSELF-LEAVE message for group " + group + ".");
				
			}
				
		} catch(Exception e) {
			
			e.printStackTrace();
			
			System.exit(1);
			
		}
		
	}
	
	private void getUserCommand() {
		
		// Get the input.
		char command[] = new char[1024];
		int inputLength = 0;
		
		try {

			inputLength = this.inputKeyboard.read(command);
			
		} catch(IOException e) {
			
			e.printStackTrace();
			
			System.exit(1);
			
		}
		
		// Setup a tokenizer for the input.
		StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(new String(command, 1, inputLength - 1)));
		
		// Check what it is.
		SpreadMessage message;
		char buffer[];
		
		try {
			
			switch(command[0]) {
			
			//JOIN
			case 'j':
				
				if (this.group == null) {
				
					// Join the group.
					if (tokenizer.nextToken() != tokenizer.TT_EOF) {
						
						this.group = new SpreadGroup();
						this.group.join(this.connection, tokenizer.sval);
						
						System.out.println("Joined " + this.group + ".");
						
					} else
						System.err.println("No group name.");
					
				} else
					System.err.println("Leave the current group before try entering another one.");
				
				break;
				
			//LEAVE
			case 'l':
				
				// Leave current group.
				if (this.group != null) {
					
					System.out.println("Left " + group + ".");
					
					this.group.leave();
					this.group = null;
					
				} else
					System.err.println("No group to leave.");
				
				break;
				
			//SEND
			case 's':
				
				// Send a new message to a group.
				message = new SpreadMessage();
				message.setSafe();
				
				// Add the groups.
				while (tokenizer.nextToken() != tokenizer.TT_EOF)
					message.addGroup(tokenizer.sval);
				
				if (message.getGroups().length > 0) {

					// Get the message.
					System.out.print("Enter message: ");
					
					buffer = new char[100];
					inputLength = this.inputKeyboard.read(buffer);
					
					String groupMessage = "&0&" + new String(buffer, 0, inputLength - 1);
					
					message.setData(groupMessage.getBytes());
					
					// Send it.
					this.connection.multicast(message);
					
					// Increment the sent message count.
					this.numberOfMessagesSent++;
					
					// Show how many were sent.
					///////////////////////////
					System.out.println("\nNumber of messages sent: " + this.numberOfMessagesSent + ".");
					
				} else
					System.err.println("You must enter the name of the group(s) that you want send the message.");				
				
				break;
				
			//CHECK
			case 'c':
				
				if (this.group != null) {
				
					System.out.println("This is " + this.serverName + ", member of the group " + this.group.toString() + ", having priority " + this.priority + "\n");
					System.out.println("The current size of this group is " + this.tmServers.size());
					System.out.println("The current Master of this group is " + this.master + ", having priority " + String.valueOf(this.tmServers.get(this.master)) + "\n");
					
					System.out.println("ALL SERVERS:");
					for (String key : this.tmServers.keySet())
						System.out.println("(" + key + "; PRIORITY: " + String.valueOf(this.tmServers.get(key)) + ")");
					
				} else
					System.err.println("You must join a group first.");
					
				
				break;
				
			//QUIT
			case 'q':
				
				// Disconnect.
				connection.disconnect();
				
				// Quit.
				System.exit(0);
				
				break;
				
			default:
				
				// Unknown command.
				System.err.println("Unknown command.");
				
				// Show the menu again.
				showMenu();
				
			}
			
		} catch(Exception e) {

			e.printStackTrace();
			
			System.exit(1);
			
		}
		
	}

	private void showMenu() {

		// Show menu.
		System.out.print("\n" +
						 "============\n" +
						 "Server Menu:\n" +
						 "============\n" +
						 "\n" +
						 "\tj <group> -- join a group\n" + 
						 "\tl -- leave current group\n" +
						 "\ts <group_1> ... <group_n> -- send a message to one (or more) group\n" +
						 "\n" +
						 "\tc -- check server status\n" +
						 "\n" +
						 "\tq -- quit\n\n");
		
	}

	public final static void main(String[] args) {
		
		// Default values.
		String address = null;
		int port = 0;
		String server = null;
		int priority = 0;
		
		if (args.length == 8) {
			
			// Check the args.
			for(int i = 0; i < args.length; i++) {
				
				// Check for address.
				if ((args[i].compareTo("-a") == 0) && (args.length > (i + 1))) {
					
					// Set the address.
					i++;
					address = args[i];
					
				}
				
				// Check for port.
				else if ((args[i].compareTo("-p") == 0) && (args.length > (i + 1))) {
					
					// Set the port.
					i++;
					port = Integer.parseInt(args[i]);
					
				}
				
				// Check for server.
				else if((args[i].compareTo("-s") == 0) && (args.length > (i + 1))) {
					
					// Set server.
					i++;
					server = args[i];
					
				} 
				
				// Check for priority.
				else if((args[i].compareTo("-r") == 0) && (args.length > (i + 1))) {
					
					// Set priority.
					i++;
					priority = Integer.parseInt(args[i]);
					
				}
				
				else {
					
					System.out.print("Usage: java Server\n" + 
							 "\t[-a <address>]     : the name or IP for the spread daemon\n" +
							 "\t[-p <port>]        : the port for the spread daemon\n" +
							 "\t[-s <server name>] : unique server name\n" +
							 "\t[-r <priority>]    : the priority of the server\n");
					
					System.exit(0);
					
				}
				
			}
			
		} else {
			
			System.out.print("Usage: java Server\n" + 
					 "\t[-a <address>]     : the name or IP for the spread daemon\n" +
					 "\t[-p <port>]        : the port for the spread daemon\n" +
					 "\t[-s <server name>] : unique server name\n" +
					 "\t[-r <priority>]    : the priority of the process\n");
			
			System.exit(0);
			
		}
		
		Server s = new Server(address, port, server, priority);
		
	}

}
/**
 * 
 */
package memdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author Susan
 * 
 * Initially thought command NUMSEQUALS was to return the list of keys.  
 * So I left my original data structure and added command NAMESEQUALTO to access this information.
 * Given that NUMEQUALTO was the original request, I implemented NUMEQUALTO independent of the implementation for NAMESEQUALT.
 * 
 * As per assignment (with modification listed above) at:
 * 		https://www.thumbtack.com/challenges/simple-database
 * 
 * Commands:
 *   GET key				print value for given key
 *   SET key value			associate value with given key
 *   NUMEQUALTO value		print number of keys with given value
 *   NAMESEQUALTO value		print keys with given value		(command not in original assignment)
 *   BEGIN					start transaction -- transactions can be nested
 *   ROLLBACK				rollback current transaction (changes in current transaction discarded). Will print out if no such transaction.
 *   COMMIT					commit changes in all active transactions.  All transactions closed.  Will print out if not in transaction.
 *   END					Terminate program
 *   
 * 
 *
 */
public class MemDb {

	private DbStates dbStates = new DbStates();
	
	/**
	 * Read standard in to execute db commands.  Commands are case insensitive.  Keys and Values are case sensitive.
	 * 
	 * @param args	not used.  Input is coming from either std in, or std via file redirection (i.e. < file).
	 * 
	 */
	public static void main(String[] args) {

		String response = "";
		MemDb db = new MemDb();
		String cmd;
		
		try (BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in))){
			while ((cmd = bufferRead.readLine()) != null) {
				cmd = cmd.trim();				
				if (cmd.isEmpty()) continue;
				
				response = db.doCommand(cmd);
				if (response == null) break;
				if (!response.isEmpty()) System.out.println(response);
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Execute db command.  Commands are case insensitive.  Keys and Values within command are case sensitive.
	 * 
	 * @param cmd	Command to process.
	 * 
	 */
	private  String doCommand(String cmd) {
		
		String [] token = cmd.trim().split(" +", 3);	// will allow multiple blanks between command args
		if (token.length == 0) return "";
		
		switch (token[0].toUpperCase()) {
		case "GET":	
			return (token.length == 2)? doGet(token[1]) : "Invalid GET command: " + cmd;
		case "SET":
			return (token.length == 3)? doSet(token[1], token[2]) : "Invalid SET command: " + cmd;			
		case "UNSET":
			return (token.length == 2)? doUnset(token[1]) : "Invalid UNSET command: " + cmd;			
		case "NUMEQUALTO":
			return (token.length == 2)? doNumEqualTo(token[1]) : "Invalid NUMEQUALTO command: " + cmd;			
		case "END":
			return (token.length == 1)? null : "Invalid END command: " + cmd;
		case "NAMESEQUALTO":
		  	return (token.length == 2)? doNamesEqualTo(token[1]) : "Invalid NAMESEQUALTO command: " + cmd; 
		  	
		case "BEGIN":
			return (token.length == 1)? doBegin() : "Invalid BEGIN command: " + cmd;
		case "ROLLBACK":
			return (token.length == 1)? doRollback() : "Invalid ROLLBACK command: " + cmd;
		case "COMMIT":
			return (token.length == 1)? doCommit() : "Invalid COMMIT command: " + cmd;
			
		default:
			return "UNKNOWN command: " + cmd;
		}

	}

	private String doBegin() {
		dbStates.beginTran();
		return "";
	}
	private String doRollback() {
		return (dbStates.rollbackTran())? "": "NO TRANSACTION";
	}
	private String doCommit() {
		return (dbStates.commitTran())? "": "NO TRANSACTION";
	}
	private  String doNamesEqualTo(String value) {
		MyKeyListNode keyListHead = dbStates.findDbKeys(value);
		if (keyListHead == null || keyListHead.prev == null) return " ";	// print out a blank line. If return "", line will not be printed
		
		StringBuilder sb = new StringBuilder(keyListHead.key);
		
		for (MyKeyListNode curKey = keyListHead.next; curKey != keyListHead && curKey != null; curKey = curKey.next) {
			sb.append(", ");
			sb.append(curKey.key);
		}
		return sb.toString();	
	}

	private  String doNumEqualTo(String value) {
		Integer valueCnt = dbStates.findDbCnts(value);
		return (valueCnt == null)? "0": valueCnt.toString();		

	}

	private  String doUnset(String key) {
		dbStates.unsetKey(key);
		return "";		// No response
	}


	private String doSet(String key, String value) {
		dbStates.setKey(key, value);
		return "";		// No response
	}

	private  String doGet(String key) {
		MyValueNode node = dbStates.findDbData(key);
		return (node == null || node.value.isEmpty())? "NULL": node.value;
	}

	/*------------------------------------------------------------------------------*/

	
	class MyValueNode {
		private String value;
		private MyKeyListNode node;
		
		public MyValueNode(String val, MyKeyListNode n) {
			value = val;
			node =n;
		}
	}
	/*------------------------------------------------------------------------------*/	
	/**
	 * Circular double linked list of keys.  This class be used as the value of the dbKeys hashMap,
	 * if the actual names want to be returned.  Where the 
	 * @author Susan
	 *
	 */
	class MyKeyListNode {
		private MyKeyListNode next;
		private MyKeyListNode prev;
		private String key;
		
		/*create an empty list*/
		public MyKeyListNode() {
			key ="";
			next=null;
			prev=null;
		}
		
		public MyKeyListNode(String newkey) {
			key = newkey;
			next = this;
			prev = this;
		}
		
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null) return false;
			if (! (o instanceof MyKeyListNode) ) return false;
			
			MyKeyListNode myNode = (MyKeyListNode)o;
			if (this.key != null) return this.key.equals(myNode.key);
			if (myNode.key != null) return myNode.key.equals(this.key);
					
			
			return true;		// both null keys
		}
		
		/**
		 * Create a new Node with the given key and add it to the tail of this list.
		 * 
		 * @param newkey	Key to create a new node for
		 */
		public MyKeyListNode addNode(String newkey) {
			if (this.prev == null) {	// empty list, so this is no longer empty after add
				this.prev = this;
				this.next = this;
				this.key = newkey;
				return this;
			}
			MyKeyListNode node = new MyKeyListNode(newkey);
			
			MyKeyListNode tail = this.prev;			
		
			tail.next = node;
			this.prev = node;
			node.next = this;
			node.prev = tail;
			
			return node;
		}
		
		/**
		 * Remove this node from it's list and return the following node.  
		 * Note: if this list has only 1 node, then list is no longer circular -- next/prev=null.
		 * 
		 * Note: remember if node is head of hash
		 * This method can only be used if you know that the node is not a copy from another list
		 * 
		 * @return the node following this node -- if empty list, will return a node with null next/prev
		 */
		public MyKeyListNode removeNode() {
			if (this.prev == null) return this;		// already empty list
			
			if (this.prev == this) { // only one node in list
				this.prev = null;	
				this.next = null;	 // denotes an empty list
				return this;
			}
			
			MyKeyListNode nodeNext = this.next;
			nodeNext.prev = this.prev;
			this.prev.next = nodeNext;
			
			return nodeNext;
		}	
		
		public  MyKeyListNode remove(String keyToRemove){
			if (this.prev == null) return this;		// already empty list
			if (keyToRemove == null) return this;
			
			if (this.prev == this) { // only one node in list
				if (keyToRemove.equals(this.key) ) {
					this.prev = null;	
					this.next = null;	 // denotes an empty list
				}
				return this;
			}
			
			MyKeyListNode nodeToRemove = null;
			if (!keyToRemove.equals(this.key)) {			
				for (MyKeyListNode curNode = this.next; curNode != this && curNode!=null; curNode = curNode.next) {
					if (keyToRemove.equals(curNode.key)) { nodeToRemove = curNode; break; }
				}
			} else {
				nodeToRemove = this;
			}
			
			
			MyKeyListNode nodeNext = null;
			if (nodeToRemove != null) {				
				nodeNext = nodeToRemove.next;
				nodeNext.prev = nodeToRemove.prev;
				nodeToRemove.prev.next = nodeNext;
			}
			
			return (nodeToRemove != this)? this: nodeNext;
		}
		
		public MyKeyListNode deepCopy() {
			MyKeyListNode newList = new MyKeyListNode(this.key);
			for (MyKeyListNode curNode = this.next; curNode != this && curNode!=null; curNode = curNode.next) {
				newList.addNode(curNode.key);
			}
			
			return newList;
		}
		
		
		public MyKeyListNode deepCopyWithout(MyKeyListNode removeNode) {
			
			
			// initially newList is the head (or an empty list if the head is the element to remove
			MyKeyListNode newList = (this.equals(removeNode))? new MyKeyListNode(): new MyKeyListNode(this.key);
			
			for (MyKeyListNode curNode = this.next; curNode != this && curNode!=null; curNode = curNode.next) {
				if (!curNode.equals(removeNode)) newList.addNode(curNode.key);
			}
			
			return newList;
		}
	}
	
	/*------------------------------------------------------------------------------*/

	class DbStates {
		/* List of all nested transaction data, where index=0 is for data outside a transaction (i.e. commmitted data) 
		 * For transaction data at index >0, the data stored is only the changes made at that given transaction level
		 */
		private ArrayList<TransData> transData = new ArrayList<TransData>();
		
		class TransData {

			private static final int DB_INITIALSIZE = 127;
			
			/*
			 * Provides look up for a value for a given key at a specific transaction level
			 * 
			 * Note: this MyValueNode was provided as a map value for dbData initially to provide fast removal of this
			 * node (ie. could be removed without traversing the entire list) as found in dbKeys.
			 * 
			 * However, when executing transactions, it was possible for the node in DbData to be a copy of the 
			 * corresponding node in dbKeys.  This it is likely that dbData should be simplified to <String, String>.			 *
			 */
			private HashMap<String, MyValueNode> dbData = new HashMap<String, MyValueNode>(DB_INITIALSIZE);
			
			/*
			 * Provides look up of keys for a given value at a specific transaction level.
			 * 
			 * Note: the value of dbKeys is a circular doubly linked list.  To handle transactions 
			 * (specifically UNSET of data which may (or may not be committed), an empty listed is represented
			 * by a node (not circular) in which next and prev = null.  
			 */
			private HashMap<String, MyKeyListNode> dbKeys = new HashMap<String, MyKeyListNode>(DB_INITIALSIZE);
			
			/* Provides look of number of keys for a given value at a specific transaction level. */
			private HashMap<String, Integer> dbCnts = new HashMap<String, Integer>(DB_INITIALSIZE);
		}
		
		public DbStates() {
			transData.add(new TransData());
		}
		
		public void beginTran() {
			transData.add(new TransData());			// add new transaction level with no changes at that level
		}
		

		/**
		 * rollback the current transaction (i.e. remove its data). 
		 * 
		 * @return	false, if not in a transaction, else return true.
		 */
		public boolean rollbackTran() {
			int curTranIdx = transData.size()-1;
			if (curTranIdx < 1) return false;		// idx ==0 is not in a transaction
			
			transData.remove(curTranIdx);
			return true;
		}
		
		/**
		 * Starting with the most recent changes (i.e. deepest transaction level), add changes to the transaction level 0 
		 * (i.e. commited changes).  Ignore changes to the same key at a shallower level, if already saved.
		 * 
		 * @return	false if not in a transaction, otherwise return true.
		 */
		public boolean commitTran() {
			int curTranIdx = transData.size()-1;
			if (curTranIdx < 1) return false;		// idx ==0 is not in a transaction
			
			HashSet<String> committedKeys = new HashSet<String>();
			HashSet<String> committedCnts = new HashSet<String>();
			HashSet<String> committedData = new HashSet<String>();
			
			for (int i=transData.size()-1; i>=1; i--) {
				for (Map.Entry<String, MyValueNode> pair: transData.get(i).dbData.entrySet()) {
					if (!committedData.contains(pair.getKey())) {
						transData.get(0).dbData.put(pair.getKey(), pair.getValue());
						committedData.add(pair.getKey());
					}
				}
				for (Map.Entry<String,MyKeyListNode>  pair: transData.get(i).dbKeys.entrySet()) {
					if (!committedKeys.contains(pair.getKey())) {
						transData.get(0).dbKeys.put(pair.getKey(), pair.getValue());
						committedKeys.add(pair.getKey());
					}
				}
				for (Map.Entry<String, Integer> pair: transData.get(i).dbCnts.entrySet()) {
					if (!committedCnts.contains(pair.getKey())) {
						transData.get(0).dbCnts.put(pair.getKey(), pair.getValue());
						committedCnts.add(pair.getKey());
					}
				}
				
				transData.remove(i);
			}
			return true;
		}

		/**
		 * Return number of keys for the given value
		 */
		private Integer findDbCnts(String value) {
			for (int i=transData.size()-1; i>=0; i--) {
				Integer valueCnt = transData.get(i).dbCnts.get(value);
				if ( valueCnt != null) return valueCnt;
			}
			return null;
		}
		/**
		 * Find the data corresponding to the given key, searching across all transactions 
		 * starting at the deepest transaction level.
		 */
		private MyValueNode findDbData(String key) {
			for (int i=transData.size()-1; i>=0; i--) {
				MyValueNode valNode = transData.get(i).dbData.get(key);
				if ( valNode != null) return valNode;
			}
			return null;
		}

		/**
		 * Find the list of keys corresponding to the given value, searching across all transactions 
		 * starting at the deepest transaction level.
		 */
		private MyKeyListNode findDbKeys(String value) {
			for (int i=transData.size()-1; i>=0; i--) {
				MyKeyListNode keyList = transData.get(i).dbKeys.get(value);
				if ( keyList != null) return keyList;
			}
			return null;
		}
		
		/**
		 * Unset the key within the data for the current transaction level.
		 * Note: unset data in keylist is a non-circular list with a null for next and prev.
		 * 
		 * @param key	key to unset.
		 */
		private  void unsetKey(String key) {
			if (key == null) return;
			
			MyValueNode valueNode = findDbData(key);	
			

			// Note: an empty value implies an unset
			if (valueNode == null || valueNode.value.isEmpty()) return;
			
			TransData curTrans = transData.get(transData.size()-1);
			
			String value = valueNode.value;
			Integer valueCnt = findDbCnts(value);
			if (valueCnt != null && valueCnt != 0) {	// valueCnt should never be null, and never be 0 if found
				curTrans.dbCnts.put(value, valueCnt-1);
			}
			
			curTrans.dbData.put(key, new MyValueNode("", null));	// empty String denotes UNSET
			
			
			// Extra feature -- following done to keep track of keys with same value
			// If the keys are in the current transaction, can modify directly
			// else if the keys are associated with a higher level transaction, then will need to deep copy the keys
			MyKeyListNode keyList = findDbKeys(value);
			
			/*
			 * Originally, tried to remove the node by calling
			 * 		MyKeyListNode nextNode = valueNode.node.removeNode();
			 * However when in transactions, valueNode.node might not be in keyList, but 
			 * instead is a copy of a node in the list.
			 * 
			 * It should be noted, that if the found keyList is not at the current transaction level, then the
			 * list needs to be copied, rather than modified in place.
			 */
			if (curTrans.dbKeys.get(value) !=null) { 
				curTrans.dbKeys.put(value, keyList.remove(key));
				
			} else {
				curTrans.dbKeys.put(value, keyList.deepCopyWithout(valueNode.node));
			}
		}
		
		/**
		 * Set the key within the data for the current transaction level.
		 * Note: if the key was previously set at any transaction level, the key is first unset, and then set.
		 */
		private void setKey(String key, String value) {
			// this should never happen from command line. Is this correct behavior?
			if (value == null) {doUnset(key); return;} 
			
			MyValueNode node = findDbData(key);			
			if (node!=null) {
				if ( value.equals(node.value)) return;	// nothing to do -- no change
				unsetKey(key);	// this will decrement/unlink for old value, new value work to follow
			}
			
			TransData curTrans = transData.get(transData.size()-1);
			
			// Increment (or add) cnts
			Integer valueCnt = findDbCnts(value);	
			if (valueCnt == null) {
				curTrans.dbCnts.put(value, 1);
			} else {
				curTrans.dbCnts.put(value, valueCnt+1);
			}			
			
			/*
			 * It should be noted, that if the found keyList is not at the current transaction level, then the
			 * list needs to be copied, rather than modified in place.
			 */

			MyKeyListNode newNode;
			MyKeyListNode keyList = findDbKeys(value);
			if (keyList == null) {
				newNode = new MyKeyListNode(key);
				curTrans.dbKeys.put(value, newNode);
			} else if (curTrans.dbKeys.get(value) !=null) {
				newNode = keyList.addNode(key);		// list already in curTrans
			} else {
				newNode = keyList.deepCopy();
				newNode.addNode(key);	
				curTrans.dbKeys.put(value, newNode);
			}

			
			curTrans.dbData.put(key, new MyValueNode(value, newNode));
		}

	}
}

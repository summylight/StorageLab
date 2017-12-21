package lab;

import java.io.*;

class bFrame {
	private static final int FrameSize=4096;
    byte[] field = new byte[FrameSize];
    
} 

class diskmanager {
	  private final static int page_size=4096;
	  private int[] Use_Record=new int[page_size];
	  private String file;
	  
	  public void GetFileName(String filename) {
		  file=filename;
	  }
	  
/*	  public void test_readPage(int page_id)throws Exception {
		  RandomAccessFile raf = Seek(0, page_id*page_size);
		  for(int i=0;i<page_size;i++) {
			  System.out.println(raf.readByte());
		  }
		  raf.close();
	  }
*/	  
    //return object[]
    public RandomAccessFile OpenFile(String filename)throws Exception{
  	  RandomAccessFile raf = new RandomAccessFile(filename, "rw");
  	  return raf;
  	  }
    
    //Fake CloseFile ......
    public void CloseFile(RandomAccessFile raf) throws Exception{
  	  raf.close();
    }
    
    //Read  the page and return byte[page_size]
    public byte[] ReadPage(int page_id) throws Exception{
  	  byte[] page=new byte[page_size];
  	  RandomAccessFile raf=OpenFile(file);
  	  for(int i=0;i<page_size;i++) {
  		  page[i]=raf.readByte();
  	  }
  	  return page;
    }     
    //Write frame into page of page_id ,return change numbers count
     public int WritePage(int page_id,bFrame frm) throws Exception{
  	   RandomAccessFile raf=Seek(0, page_id*page_size);
  	   int count=0;
  	   byte tmp;
  	   for(int i=0;i<page_size;i++) {
  		   tmp=raf.readByte();
  		   if(tmp!=frm.field[i]){
  			   RandomAccessFile back=Seek(i, page_id*page_size);
  			   back.write(frm.field[i]);
  			   back.close();
  			   count++;
  		       }
  	   }
  	 RandomAccessFile randomFile = new RandomAccessFile("D:/trace.txt", "rw");
     // 文件长度，字节数
     long fileLength = randomFile.length();
     //将写文件指针移到文件尾。
     randomFile.seek(fileLength);
     randomFile.writeBytes("page "+(page_id+1) +" is writing to disk\t\n");
     randomFile.close();
//  	   System.out.println("Now write the page "+page_id)
  	   raf.close();
  	   return count;
     }
     
     //seek pos and offset to return a RF
     public RandomAccessFile Seek(int offset, int pos) throws Exception{
  	   RandomAccessFile raf=OpenFile(file);
  	   raf.seek(pos+offset);
  	   return raf;
     }
     
     //return current filename
     public void GetFile(RandomAccessFile raf){
  	   System.out.println(file);
     }
     
     // raf increment a page 
     public RandomAccessFile IncNumPages(RandomAccessFile raf) throws Exception{
  	  int page_id=(int)(raf.getFilePointer()%page_size);
  	  page_id=page_id+1;
  	  raf.seek(page_id*page_size);
  	  return raf;
     }
     
     //return page_id of raf
     public int GetNumPages(RandomAccessFile raf) throws Exception{
  	   int page_id=(int)(raf.getFilePointer()%page_size);
  	   return page_id;
     }
     
     //set the record of page_id to use_bit
     public void SetUse(int page_id,int use_bit) {
  	   Use_Record[page_id]=use_bit;
     }
     
     //return the record of page_id
     public int GetUse(int page_id) {
  	   return Use_Record[page_id];
     }


     
}
//implements BCB ,except latch and count 
class BCB{
	  int page_id;
	  int frame_id;
	  int latch;
	  int count;
	  boolean dirty;
	  protected BCB next=null;
	  public BCB(){
		  page_id=0;
		  frame_id=0;
		  latch=0;
		  count=0;
		  dirty=false;
	  }
}

//implement LRU on LinkList
class LRUNode{
	protected LRUNode next=null;
	protected BCB CRLBlock;
	public LRUNode(BCB B){
		this.CRLBlock=B;
	}
	public LRUNode() {
	}
}

class LRUList{
	protected  LRUNode First=new LRUNode();  //头结点
	private  int count=0;
	
	public void insert(BCB B){ //insert a node into the head of list
		LRUNode node=new LRUNode(B);
		node.next=First.next;
		First.next=node;
		count++;
	}
	
	public void delete(int frame_id){ //delete node of frame_id
		LRUNode tmp=First;
		while (tmp.next.CRLBlock.frame_id!=frame_id) {
			tmp=tmp.next;
		}
		tmp.next=tmp.next.next;
		count--;
    }
	
	public BCB LRUrm(){   //delete the node according to LRU,return the BCB
		LRUNode prior=First;
		LRUNode cur=prior.next;
		while(cur.next!=null) {
			prior=cur;
			cur=cur.next;
		}
		BCB B=cur.CRLBlock;
		prior.next=prior.next.next;
		count--;
		return B;
	}
	
	public int getnum() { //return the number of existing BCB
		return count;
	}
}

/*buffer to buffer
 * ptofhash to translate page_id into BCB
 * ftophash to translate frame_id to page_id, initialize to all -1 
 * LRU to control BCB LRU
 * Version 17.12.20
 * 
 */
class buffer {
	public final static int buffer_size=1024;
	public final static int frame_size=4096;
	private BCB[] ptofhash=new BCB[buffer_size];
	private bFrame[] FrameBuf =new bFrame[buffer_size];
	private LRUList LRU=new LRUList();
	private int[] ftophash=new int[buffer_size];
	private diskmanager DM=new diskmanager();
	
	public buffer(String filename) {
		for(int i=0;i<buffer_size;i++) {
			ftophash[i]=-1;
		}
		DM.GetFileName(filename);
	}
	
	//Test Write into page
	public void TestWrite(int page_id)throws Exception {
		int frame_id=FixPage(page_id);
		BCB B=seekBCB(page_id);
		B.dirty=true;
		
		bFrame b=FrameBuf[frame_id];
		for(int i=0;i<frame_size;i++) {
			b.field[i]=(byte)(i%100);
		}
//		for(byte j:b.field)
	//		System.out.println(j);
	}

	//return frame_id of page_id ,if page do not exist then choose victim frame
	public int FixPage(int page_id) throws Exception{
		BCB B;
		int frame_id=-1;
		for(int i=0;i<buffer_size;i++) {
			if(ftophash[i]==page_id) {
				frame_id=i;
				break;
			}
		}
		if(frame_id!=-1) {//page exist ,resort LRU and return  
			B=seekBCB(page_id);
			LRU.delete(B.frame_id);
			LRU.insert(B);
		}
		else {                      //if page don't exist then insert the page and resort LRUlist
			B=new BCB();            //create BCB and insert it into BCB hash table
			B.page_id=page_id;
			int hash=page_id%buffer_size;
			if(ptofhash[hash]==null) ptofhash[hash]=B;//the location of hash table is empty
			else {                     //insert the BCB into the list of hash table
			BCB tmp;
			tmp=ptofhash[hash];
				while (tmp.next!=null) {
					tmp=tmp.next;
				};
				tmp.next=B;
			}
			if(LRU.getnum()==buffer_size) { //LRU full , victim one frame 
				B.frame_id=SelectVictim();
				ftophash[B.frame_id]=page_id;
			}
			else {                          // no full ,select a free frame
				for(int i=0;i<buffer_size;i++) {
				if(ftophash[i]==-1) {
					B.frame_id=i;
					ftophash[i]=B.page_id;
					break;
				}
			}
			}
			LRU.insert(B);                  //insert BCB into LRU
			/*write content of page into frame*/
			FrameBuf[B.frame_id]=new bFrame();
			FrameBuf[B.frame_id].field=DM.ReadPage(page_id);		
		}
//		System.out.println("LRU:"+LRU.getnum());
		return B.frame_id;
	}
	//???I do not mean it ,but  I think this lab do not need FixNewPage and UnFixPage?? 
	public void FixNewPage() {		
	}
	
	
	public void UnFixPage(int page_id) {		
	}
	
	//buffer_size decrease the number of LRU 
	public int NumFreeFrames() {
		return buffer_size-LRU.getnum();
	}
	
	//clear a frame according to LRU list and remove the BCB
	public int SelectVictim() throws Exception{
		BCB B=LRU.LRUrm();
		int frame_id=B.frame_id;
		if(B.dirty) DM.WritePage(B.page_id, FrameBuf[B.frame_id]);  //write dirty page
		/*delete  BCB from BCB hash table */
		BCB pre=ptofhash[B.page_id%buffer_size];
		if(pre==B) B=null;
		else {
			while (pre.next!=B) pre=pre.next;
			pre.next=B.next;
		}
		return frame_id;
	}
	//return BCB according to page_id 
	private BCB seekBCB(int page_id) {
		BCB pre=ptofhash[page_id%buffer_size];
		if(pre.page_id==page_id) return pre;
		else {
			while(pre.page_id!=page_id&&pre!=null) pre=pre.next;
			return pre;
		}
	}
	//search the page, if it exist ,return frame_id ,else return -1
	public int HashPtoF(int page_id) {
		
		if(seekBCB(page_id)==null) return -1;
		else return seekBCB(page_id).frame_id;
	}
	
	//remove the BCB of page_id 
	public void RemoveBCB(BCB ptr, int page_id) {
		BCB pre=ptofhash[page_id%buffer_size];
		if(ptr==pre) ptofhash[page_id%buffer_size]=null ;
		else {
			while(ptr!=pre.next&&pre.next!=null) pre=pre.next;
			pre.next=pre.next.next;
		}
	}
	
	//remove the node of frame_id from LRU 
	public void RemoveLRUEle(int frame_id) {
		LRU.delete(frame_id);
	}
	
	public void SetDirty(int frame_id) {
		int page_id=ftophash[frame_id];
		BCB cur=seekBCB(page_id);
		cur.dirty=true;
	}
	
	public void UnsetDirty(int frame_id) {
		int page_id=ftophash[frame_id];
		BCB cur=seekBCB(page_id);
		cur.dirty=false;
	}
	
	//check ALL BCB to write dirty  
	public int WriteDirtys() throws Exception{
		BCB tmp;
		LRUNode L=LRU.First;
		int count=0;
		for(int i=0;i<LRU.getnum();i++) {
			tmp=L.next.CRLBlock;
			if(tmp.dirty) 
			count=count+DM.WritePage(tmp.page_id, FrameBuf[tmp.frame_id]);
//			System.out.println(tmp.page_id);
			L=L.next;
//			DM.test_readPage(tmp.page_id);
		}
		return count;
		//System.outprintln("Dirty count is" + count);
	}
	
	public void PrintFrame(int frame_id) {
		bFrame frame=FrameBuf[frame_id];
		for(int i=0;i<frame_size;i++) {
			System.out.print(frame.field[i]);
		}
	}
	
}


public class test {
	public static void main(String[] args) {
		int request=0;
		int page_id=0;
		long count=0;
		try {test database=new test();
		String filename;
		database.createfile();
		//BufferedReader br = new BufferedReader(new InputStreamReader(System.in)); 
        //System.out.println("Enter testfile location :"); 
        //filename = br.readLine(); 
		
        filename="D:/data-5w-50w-zipf.txt";
  //      filename="D:/test_write.txt";
		buffer BUF=new buffer("D:/data.dbf");
        File f1 = new File(filename);
        BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(f1)),100 * 1024 * 1024); 
		 for (String line = br1.readLine(); line != null; line = br1.readLine()) {  
			 String[] num=line.split(",");
			 request= Integer.parseInt(num[0]);
			 page_id=Integer.parseInt(num[1]);
			 page_id--;
			 if(request==1) { //write
				 BUF.TestWrite(page_id);
			 }
			 else {           //read
				 BUF.FixPage(page_id);
			 }
//			 System.out.println(1);
			 count++;
//			 System.out.println(request+" "+page_id+" "+count);
			 System.out.println(count);
		 }
//		 System.out.println(request+" "+page_id+" "+count);
		 BUF.WriteDirtys();
		 br1.close();
		 System.out.println("Success!!");
//		 database.readfile();
		}catch(Exception e) {
			e.printStackTrace();
			System.out.println(request+" "+page_id+" "+count);
		}
		
	}
	public void createfile(){
  	  int page_num=50000;
  	  try {
  		  RandomAccessFile raf = new RandomAccessFile("D:/data.dbf", "rw");  
  		  System.out.println("Create database success!!");
  	  raf.setLength(page_num*4096);
  	  raf.close();
  	  }catch(IOException e1){
		System.out.println("Fail to create database");
	}
  	 }
	public void readfile() {
		
		try {RandomAccessFile raf=new RandomAccessFile("D:/data.dbf", "rw");
			byte i=0;
			long j=raf.length();
			while((j--)>0) {
				i=raf.readByte();
				System.out.print(i);
			}raf.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
}


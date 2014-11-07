#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;	//frameNo is the same as the index of bufTable
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	// Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
	//Status status;
	for(int i = 0; i < numBufs; i++){
		if(bufTable[i].valid && bufTable[i].dirty){
			//flush, performs on bufPool (a Page instance)
			bufTable[i].file->writePage(bufTable[i].pageNo, &(bufPool[i]));
		}
	}
	cout<<"down here!"<<endl;	
	delete [] bufTable;
	cout<<"down here2!"<<endl;
	delete [] bufPool;
	//delete hashTable;
	
}


const Status BufMgr::allocBuf(int & frame) {
	//Add a variable to count for pin number for a iteration and clear it after one iteration
	//cout<<"Allocating "<<frame<<" frame"<<endl;
	int pdFrame = 0;
	int initPos = clockHand;
	Status status;
	BufDesc* curFrame;
	while(true){
		advanceClock();
		frame = clockHand;
		//cout<<"Allocating "<<frame<<" frame (in process)!"<<endl;
		//cout<<numBufs<<" buf size!"<<endl; 
		curFrame = &(bufTable[frame]);
		//frame not having a valid page: empty
		if(!curFrame->valid)	break;
		//frame has a valid page
		else{
			//cout<<"Frame Occupied!"<<endl;
			if(curFrame->refbit)
				curFrame->refbit = false;	//and advance clock
			else{
				//cout<<"Not ref!"<<endl;
				if(!curFrame->pinCnt){
					//cout<<"Not Pinned!"<<endl;
					//status = hashTable->remove(curFrame->file, curFrame->pageNo);
					//if(status != OK) return status;
					if(curFrame->dirty){	//flushes page to Disk if dirty
						//cout<<"Dirty!"<<endl;
						//status = hashTable->remove(curFrame->file, curFrame->pageNo);
						//if(status != OK)	
							//return status;
						status =  curFrame->file->writePage(curFrame->pageNo, &(bufPool[frame]));
						if(status != OK)
							return status;
						curFrame->dirty = false;
					}
					//status = hashTable->remove(curFrame->file, curFrame->pageNo);
					//if(status != OK) return status;
					status = hashTable->remove(curFrame->file, curFrame->pageNo);
					if(status != OK) return status;
					curFrame->Clear();
					break;
				}
				//Page pinned
				else{
					//cout<<"Pinned!"<<endl;
					pdFrame++;
					if(pdFrame == numBufs)
						return BUFFEREXCEEDED;
					if(frame == initPos){
						pdFrame = 0;
					}
				}
			}
		}
	}
	//cout<<frame<<" Allocation success!"<<endl;
	return OK;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
	//cout<<"Reading page!"<<endl;
	int frameNo = -1;
	Status status = hashTable->lookup(file, PageNo, frameNo);
	//Page already in the buffer pool
	if(status == OK){
		//cout<<"In the buffer pool!"<<endl;
		//if(!bufTable[frameNo].valid) cout<<"Should be valid!";
		page = &(bufPool[frameNo]);
		bufTable[frameNo].refbit = true;
		bufTable[frameNo].pinCnt++;		
	}
	//page not in the buffer pool
	else{
		//cout<<"Not in the buffer pool!"<<endl;
		status = allocBuf(frameNo);
		if(status != OK) return status;
		page = &(bufPool[frameNo]);
		status = file->readPage(PageNo, page);
		if(status != OK) return status;
		status = hashTable->insert(file, PageNo, frameNo);
		if(status != OK) return status;
		bufTable[frameNo].Set(file, PageNo);
			
	}
	//cout<<"Read Success!"<<endl;
	return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) {
	//cout<<"Unpinning frame!"<<endl;
	int frameNo = -1;
	Status status = hashTable->lookup(file, PageNo, frameNo);
	if(status != OK) return status;
	//cout<<"Unpinning "<<frameNo<<" frame!"<<endl;
	if(!bufTable[frameNo].pinCnt) return PAGENOTPINNED;
	bufTable[frameNo].pinCnt--;
	if(dirty) 
		bufTable[frameNo].dirty = true;
	//cout<<"Unpin success!"<<endl;
	return OK;
}


const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)  {
	//cout<<"Allocating Page"<<endl;
	int frameNo = -1;
	Status status = file->allocatePage(pageNo);
	if(status != OK) return status;
	status = allocBuf(frameNo);
	if(status != OK) return status;
	status = hashTable->insert(file, pageNo, frameNo);
	if(status != OK) return status;
	bufTable[frameNo].Set(file, pageNo);
	page = &(bufPool[frameNo]);
	//load page from disk to buffer pool
	status = file->readPage(pageNo, page);
	if(status != OK) return status;
	return OK;
}


const Status BufMgr::disposePage(File* file, const int pageNo) {
	cout<<"Disposing Page!"<<endl;
	int frameNo = -1;
	Status status = hashTable->lookup(file, pageNo, frameNo);
	if(status == OK){
		cout<<"Found Page "<<pageNo<<" in Frame "<<frameNo<<" !"<<endl;
		bufTable[frameNo].Clear();
		//cout<<"BEFORE REMOVE"
		status = hashTable->remove(file, pageNo);
		if(status != OK) return status;
	}
	status = file->disposePage(pageNo);
	if(status != OK) return status;
	cout<<"Dispose success!"<<endl;
	return OK;
}


const Status BufMgr::flushFile(const File* file) {
	Status status;
	for(int i = 0; i < numBufs; i++){
		//cout<<"in flush loop"<<endl;
		if(bufTable[i].file == file){
			//cout<<"match!"<<endl;
			if(bufTable[i].pinCnt) return PAGEPINNED;
			if(bufTable[i].dirty){
				//cout<<"dirty!"<<endl;
				status = bufTable[i].file->writePage(bufTable[i].pageNo, &(bufPool[i]));
				if(status != OK) return status;
				bufTable[i].dirty = false;
			}
			//cout<<"before hash remove!"<<endl;
			//cout<<"removing "<<i<<" frame"<<endl;
			status = hashTable->remove(file, bufTable[i].pageNo);
			if(status != OK) return status;
			//cout<<"after hash remove!"<<endl;
			bufTable[i].Clear();
		}
		//cout<<"Flushing frame "<<i<<endl;
	}
	//cout<<"flush success!"<<endl;
	return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}



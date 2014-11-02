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
		if(bufTable[i].dirty){
			//flush, performs on bufPool (a Page instance)
			bufTable[i].file->writePage(bufTable[i].pageNo, &(bufPool[i]));
		}
	}	
	delete [] bufTable;
	delete [] bufPool;
	delete hashTable;
	
}


const Status BufMgr::allocBuf(int & frame) {
	//Add a variable to count for pin number for a iteration and clear it after one iteration
	int pdFrame = 0;
	int initPos = clockHand;
	Status status;
	BufDesc* curFrame;
	while(true){
		advanceClock();
		frame = clockHand;
		curFrame = &(bufTable[frame]);
		//frame not having a valid page: empty
		if(!curFrame->valid)	break;
		//frame has a valid page
		else{
			if(curFrame->refbit)
				curFrame->refbit = false;	//and advance clock
			else{
				if(!curFrame->pinCnt){
					if(curFrame->dirty){	//flushes page to Disk if dirty
						status = hashTable->remove(curFrame->file, curFrame->pageNo);
						if(status != OK)	
							return status;
						status =  curFrame->file->writePage(curFrame->pageNo, &(bufPool[frame]));
						if(status != OK)
							return status;
					}
					curFrame->Clear();
					break;
				}
				//Page pinned
				else{
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
	delete curFrame;
	return OK;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
	int frameNo = -1;
	Status status = hashTable->lookup(file, PageNo, frameNo);
	//Page already in the buffer pool
	if(status == OK){
		ASSERT(bufTable[frameNo].valid);
		page = &(bufPool[frameNo]);
		bufTable[frameNo].refbit = true;
		bufTable[frameNo].pinCnt++;		
	}
	//page not in the buffer pool
	else{
		status = allocBuf(frameNo);
		if(status != OK) return status;
		page = &(bufPool[frameNo]);
		status = file->readPage(PageNo, page);
		if(status != OK) return status;
		status = hashTable->insert(file, PageNo, frameNo);
		if(status != OK) return status;
		bufTable[frameNo].Set(file, PageNo);
			
	}
	return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) {
	// TODO: Implement this method by looking at the description in the writeup.
	int frameNo = -1;
	Status status = hashTable->lookup(file, PageNo, frameNo);
	if(status != OK) return status;
	if(!bufTable[frameNo].pinCnt) return PAGENOTPINNED;
	bufTable[frameNo].pinCnt--;
	if(dirty) 
		bufTable[frameNo].dirty = true;
	return OK;
}


const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)  {
	// TODO: Implement this method by looking at the description in the writeup.
	int frameNo = -1;
	Status status = file->allocatePage(pageNo);
	if(status != OK) return status;
	status = allocBuf(frameNo);
	if(status != OK) return status;
	status = hashTable->insert(file, pageNo, frameNo);
	if(status != OK) return status;
	bufTable[frameNo].Set(file, pageNo);
	page = &(bufPool[frameNo]);
	status = readPage(file, pageNo, page);
	if(status != OK) return status;
	return OK;
}


const Status BufMgr::disposePage(File* file, const int pageNo) {
	// TODO: Implement this method by looking at the description in the writeup.
	int frameNo = -1;
	Status status = hashTable->lookup(file, pageNo, frameNo);
	if(status != OK) return status;
	bufTable[frameNo].Clear();
	status = hashTable->remove(file, pageNo);
	if(status != OK) return status;
	status = file->disposePage(pageNo);
	if(status != OK) return status;
	return OK;
}


const Status BufMgr::flushFile(const File* file) {
	// TODO: Implement this method by looking at the description in the writeup.
	Status status;
	for(int i = 0; i < numBufs; i++){
		if(bufTable[i].file == file){
			if(bufTable[i].dirty){
				status = bufTable[i].file->writePage(bufTable[i].pageNo, &(bufPool[i]));
				if(status != OK) return status;
				bufTable[i].dirty = false;
			}
			status = hashTable->remove(file, bufTable[i].pageNo);
			if(status != OK) return status;
			bufTable[i].Clear();
		}
	}
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



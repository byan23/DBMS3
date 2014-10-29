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
	// TODO: Implement this method by looking at the description in the writeup.
	// Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
	Status status;
	for(int i = 0; i < numBufs; i++){
		if(bufTable[i].dirty){
			//flush, performs on bufPool (a Page instance)
			//Modification needed: writePage returns a STATUS
			status = bufTable[i].file->writePage(bufTable[i].pageNo, &(bufPool[i]));
		}
	}	
	delete [] bufTable;
	delete [] bufPool;
	delete hashTable;
	
}


const Status BufMgr::allocBuf(int & frame) {
	// TODO: Implement this method by looking at the description in the writeup.
	int initPos = clockHand;
	Status status;
	while(true){
		advanceClock();
		frame = clockHand;
		BufDesc* curFrame = &(bufTable[frame]);
		if(!curFrame->valid)	break;
		//frame has a valid page
		else{
			if(curFrame->refbit)
				curFrame->refbit = false;	//and advance clock
			else{
				if(!curFrame->pinCnt){
					if(curFrame->dirty){	//flushes page to Disk
						status = hashTable->remove(curFrame->file, curFrame->pageNo);
						if(status != OK)	
							return status;
						status =  curFrame->file->writePage(curFrame->pageNo, &(bufPool[frame]));
						if(status != OK)
							return status;
						curFrame->Clear();	//clear the frame
						break;
					}		
					else	break;
				}
				//Page pinned
				else{
					if(frame == initPos)
						return BUFFEREXCEEDED;
				}
			}
		}
	}
	return OK;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
	// TODO: Implement this method by looking at the description in the writeup.
	//frame needs clearing
	return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) {
	// TODO: Implement this method by looking at the description in the writeup.
	return OK;
}


const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page)  {
	// TODO: Implement this method by looking at the description in the writeup.
	//frame needs clearing
	return OK;
}


const Status BufMgr::disposePage(File* file, const int pageNo) {
	// TODO: Implement this method by looking at the description in the writeup.
	return OK;
}


const Status BufMgr::flushFile(const File* file) {
	// TODO: Implement this method by looking at the description in the writeup.
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



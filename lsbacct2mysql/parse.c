/******************************************************
* LSBLIB - Examples
* 
* get event record
* The program takes a job name as the argument and returns
* the information of the job with this given name 
******************************************************/ 

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <lsf/lsbatch.h>

int main(int argc, char **argv)
{
  char *eventFile = argv[1];
  /*location of lsb.acct*/
  
  FILE *fp;/* file handler for lsb.events */
  struct eventRec *record;
  /* pointer to the return struct of lsb_geteventrec() */
  
  int  lineNum = 0;/* line number of next event */
  char *User = argv[1];/* specified job name */
  int  i;


  struct jobFinishLog  *jobFinish;
  
  /* initialize LSBLIB and get the configuration environment */
  if (lsb_init(argv[0]) < 0) {
    lsb_perror("lsb_init");
    exit(-1);
  }
  
  /* open the file for read */
  fp = fopen(eventFile, "r");
  if (fp == NULL) {
    perror(eventFile);
    exit(-1);
  }

 
  /* get events and print out the information of the event
     records with the given job name in different format */

  for (;;) {
    record = lsb_geteventrec(fp, &lineNum);
    if (record == NULL) {
      if (lsberrno == LSBE_EOF)
        exit(0);
        lsb_perror("lsb_geteventrec");
        exit(-1);
    }
    
    jobFinish = &(record->eventLog.jobFinishLog);

    //printf("Num exec hosts = %d\n",jobFinish->numExHosts);
    if (jobFinish->numExHosts != 0) { /* this is a hack, sometimes no
host
                                             is returned */
      
      printf("%s %s %s %2.5f %2.5f %.3f %d %d %d %s \n", 
           jobFinish->userName,
           jobFinish->execHosts[0],
           jobFinish->queue,
           ((jobFinish->lsfRusage.ru_utime/60)/60),
           ((jobFinish->lsfRusage.ru_stime/60)/60),
           ((jobFinish->maxRMem)/1024)*1.0,
           jobFinish->startTime,
           jobFinish->endTime,
           jobFinish->jStatus,
           jobFinish->command
           ); 
    } 
  }
}

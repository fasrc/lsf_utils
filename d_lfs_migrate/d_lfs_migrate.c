/*
Lustre API llapi_file_get_stripe doc with example code:
	http://wiki.lustre.org/manual/LustreManual18_HTML/StripingAndIOOptions.html#50651269_11204

John Brunelle
*/

#include <libdftw.h>

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/wait.h>

#include <lustre/liblustreapi.h>
#include <lustre/lustre_user.h>

#define MAX_OSTS 1024
#define LOV_EA_SIZE(lum, num) (sizeof(*lum) + num * sizeof(*lum->lmm_objects))
#define LOV_EA_MAX(lum) LOV_EA_SIZE(lum, MAX_OSTS)

static int lfs_migrate(const char *fpath, const struct stat *sb, int tflag) {
	struct lov_user_md_v1 *lum;  //lustre stripe info
	int ostidx;                  //OST number
	
	
	pid_t pid = 0;   //process id
	int status = 0;  //child status
	int r = 0;       //function return value
	
	char *args[] = {"lfs_migrate", "-y", (char*)NULL, (char*)NULL};
	char *fpathc = NULL;

	switch (tflag) {
		case FTW_D:
			break;
		case FTW_DNR:
			fprintf(stderr, "unreadable directory: %s\n", fpath);
			break;
		case FTW_NS:
			fprintf(stderr, "unstatable file: %s\n", fpath);
			break;
		default:
			lum = malloc(LOV_EA_MAX(lum));
			if (lum==NULL) {
				fprintf(stderr, "*** ERROR *** malloc failed\n");
			} else {
				r = llapi_file_get_stripe(fpath, lum);
				if (r != 0) {
					fprintf(stderr, "*** ERROR *** llapi_file_get_stripe of [%s] failed with return value [%d]\n", fpath, r);  //docs say it sets errno, I find that's not the case
				} else {
					ostidx = lum->lmm_objects[0].l_ost_idx;
					//fprintf(stdout, "got a file: %s: obdidx: %d\n", fpath, ostidx);
				
					/*
					//(it would be better to do this only in the child, but fork() support in mpi on openib is very fragile, so keeping as much as possible in the parent)
					fpathc = (char *)malloc((strlen(fpath)+1) * sizeof(*fpathc));
					if (fpathc==NULL) { fprintf(stderr, "*** ERROR *** malloc failed\n"); exit(1); }
					strcpy(fpathc, fpath);
					args[2] = fpathc;
					*/
					
					if (ostidx<=5) {  //panoss01 == panlfs-OST000{0..5}
						fprintf(stdout, "found a file on panoss01, obdidx %d: %s\n", ostidx, fpath);

						/*
						pid = fork();
						if (pid==-1) {
							fprintf(stderr, "*** ERROR *** (parent) failed to fork, errno: %d\n", errno);
							return errno;
						}
						if (pid==0) {
							execvp(args[0], args);
							//(control only gets here if exec fails)
							_exit(errno);  //(note that errno will be ENOENT (2) if binary of library is not found, not 127 like shell uses)
						}
						else {
							waitpid(pid, &status, 0);
							if (status!=0) {
								if (WIFSIGNALED(status)) {
									fprintf(stderr, "*** ERROR *** lfs_migrate for [%s] terminated by signal: %d\n" , fpath, WTERMSIG(status));
								} else if (WIFEXITED(status)) {
									fprintf(stderr, "*** ERROR *** lfs_migrate for [%s] exited with status: %d\n"   , fpath, WEXITSTATUS(status));
								} else {
									fprintf(stderr, "*** ERROR *** lfs_migrate for [%s] ended with raw status: %d\n", fpath, status);
								}
							}
						}
						*/
					}

					/*
					free(fpathc); fpathc = NULL;
					*/
				}
			}
			free(lum); lum = NULL;
	}

    return 0;
}

int main(int argc, char **argv) {
	int rank;

    if(argc != 2) {
        fprintf(stderr, "Usage: %s <path>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    fprintf(stdout, "Walking with root as: `%s'\n", argv[1]);
    dftw(argv[1], lfs_migrate);

    exit(EXIT_SUCCESS);
}

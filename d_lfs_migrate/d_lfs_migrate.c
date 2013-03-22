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

char find_only = 1;

static int lfs_migrate(const char *fpath, const struct stat *sb, int tflag) {
	switch (tflag) {
		case FTW_D:
			return 0;
		case FTW_DNR:
			fprintf(stderr, "unreadable directory: %s\n", fpath);
			return 1;
		case FTW_NS:
			fprintf(stderr, "unstatable file: %s\n", fpath);
			return 0;
		default: {
			struct lov_user_md_v1 *lum;  //lustre stripe info
			int ostidx;                  //OST number
			
			lum = malloc(LOV_EA_MAX(lum));
			if (lum==NULL) {
				fprintf(stderr, "*** ERROR *** malloc failed\n");
				return 1;
			} else {
				int r = -1;  //return value
				r = llapi_file_get_stripe(fpath, lum);
				if (r != 0) {
					fprintf(stderr, "*** ERROR *** llapi_file_get_stripe of [%s] failed with return value [%d]\n", fpath, r);  //docs say it sets errno, I find that's not the case
					free(lum);
					return 0;
				} else {
					ostidx = lum->lmm_objects[0].l_ost_idx;
				
					if (ostidx==6) {
						if (find_only) {
							fprintf(stdout, "found a file on to migrate, on obdidx %d: %s\n", ostidx, fpath);
						} else {
							char *argv[] = {"lfs_migrate", "-n", (char*)NULL, (char*)NULL};
							pid_t pid = 0;   //process id
							int status = 0;  //child status

							argv[2] = (char*)fpath;  //(casting away const here is fine -- execvp's argv is "completely constant" and would've been declared that way if it weren't for C limitations)
							
							pid = fork();
							if (pid==-1) {
								fprintf(stderr, "*** ERROR *** (parent) failed to fork, errno: %d\n", errno);
								free(lum);
								return errno;
							}
							if (pid==0) {
								execvp(argv[0], argv);
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
						}
					}
				}
				free(lum); lum = NULL;
			}
			return 0;
		}
	}
}

int main(int argc, char **argv) {
    if(argc != 2) {
        fprintf(stderr, "Usage: %s <path>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    fprintf(stdout, "Walking with root as: `%s'\n", argv[1]);
    dftw(argv[1], lfs_migrate);

    exit(EXIT_SUCCESS);
}

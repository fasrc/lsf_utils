/*
See the embedded "man"-style string below for documentation.

Lustre API
	llapi_file_get_stripe doc with example code:
		http://wiki.lustre.org/manual/LustreManual18_HTML/StripingAndIOOptions.html#50651269_11204
	see also:
		http://wiki.lustre.org/doxygen/b_client_io_layering/api/html/structlov__ost__data__v1.html

Built upon libdftw:
	https://github.com/hpc/libdftw

John Brunelle
Harvard FAS Research Computing
*/

#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/wait.h>
#include <getopt.h>

#include <libdftw.h>

#include <lustre/liblustreapi.h>
#include <lustre/lustre_user.h>

//FIXME:
//This should use LOV_MAX_STRIPE_COUNT (from lustre/lustre_idl.h) as the 
//default, rather than hardcode its current value (as of 1.8.7), 160.
#ifndef D_LFS_MIGRATE_MAX_STRIPE_COUNT
#define D_LFS_MIGRATE_MAX_STRIPE_COUNT 160
#endif

//The maximum number of OSTs is a compile-time option of luster.  Current value 
//(as of 1.8.7) is 8150.
#ifndef D_LFS_MIGRATE_MAX_OST_COUNT
#define D_LFS_MIGRATE_MAX_OST_COUNT 8150
#endif

#define LOV_EA_SIZE(lum, num) (sizeof(*lum) + num * sizeof(*lum->lmm_objects))
#define LOV_EA_MAX(lum) LOV_EA_SIZE(lum, D_LFS_MIGRATE_MAX_STRIPE_COUNT)

static char *helpstr = \
"NAME\n"
"    d_lfs_migrate - distributed, combined lsf find and lfs_migrate\n"
"\n"
"SYNOPSIS\n"
"    d_lfs_migrate --ost-number-base BASE --ost OST_NUMBER... PATH_TO_MIGRATE\n"
"\n"
"DESCRIPTION\n"
"    This is a tool to use for evacuating data from lustre OSTs or rebalancing \n"
"    data on OSTs when adding new ones.  The standard procedure for this -- \n"
"    walking the filesystem with lfs find and passing files to migrate to \n"
"    lsf_migrate -- is a serial operation that does not take advantage of \n"
"    lustre's parallel, high-performance nature.  This tool uses libdftw and MPI \n"
"    to parallelize this procedure.\n"
"\n"
"    We've seen strong scaling efficiency of ~80% when running 20-way parallel \n"
"    on nodes already running mixed workloads, and thus far have stopped there \n"
"    without any further investigation or optimization... better performance is \n"
"    quite likely possible!\n"
"\n"
"    All the normal preparations and warnings regarding lfs_migrate apply:\n"
"\n"
"        - The files to be migrated must not be in use.\n"
"\n"
"        - The OSTs should be deactivated on the MDS (but not on clients).\n"
"\n"
"        - This does not migrate files with multiple hard links.  lfs_migrate \n"
"          can be made to, but this does not pass along those options.  See \n"
"          `lfs_migrate_hardlinks' in https://github.com/fasrc/lustre_utils/ for \n"
"          a better approach.\n"
"\n"
"    This has not been tested on striped files, but it should work.  There are \n"
"    also some limits on ost and stripe count that have compile-time overrides \n"
"    (look at the code).\n"
"\n"
"    This code does a fork/exec (when not running with --find-only), which MPI \n"
"    will usually complain about at runtime.  You can suppress this warning (in \n"
"    OpenMPI) with: \n"
"\n"
"        -mca mpi_warn_on_fork 0\n"
"\n"
"    For more on libdftw see:\n"
"\n"
"        LaFon, Misra, Bringhurst:\n"
"        `On Distributed File Tree Walk of Parallel File Systems'\n"
"        http://dl.acm.org/citation.cfm?id=2389114\n"
"\n"
"        https://github.com/hpc/libcircle\n"
"        https://github.com/hpc/libdftw\n"
"\n"
"OPTIONS\n"
"    -o, --ost OST_NUMBER\n"
"        Migrate files off of OST_NUMBER.  Specify multiple times if multiple \n"
"        OSTs are being migrated (hint: use shell brace expansion, e.g. \n"
"        -o{5..9}, to easily specify a range).\n"
"\n"
"    -b, --ost-number-base BASE\n"
"        The numeric base used to interpret OST_NUMBER.  Valid values are 10 \n"
"        and 16.  Since both are used by various other tools, this option is \n"
"        required, out of an abundance of caution.\n"
"\n"
"    -n, --find-only\n"
"        Don't actually migrate the files, just print out the names of files \n"
"        that would be migrated.  This is analagous to lfs_migrate's -n, but \n"
"        lfs_migrate is not even called in this case.\n"
"\n"
"    -h, --help\n"
"        Print this help.\n"
"\n"
"REQUIREMENTS\n"
"    lustre 1.8.7 or compatible\n"
"\n"
"AUTHOR\n"
"    John Brunelle\n"
"    Harvard FAS Research Computing\n"
;


//OST numbers to migrate off of
static int osts[D_LFS_MIGRATE_MAX_OST_COUNT];

//length of osts
static int ostsl = 0;

//base in which OST numbers are expressed
static int ost_number_base = -1;

//boolean to control find vs. actual migrate
static char find_only = 0;


//dftw callback
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
				int r = -1;
				r = llapi_file_get_stripe(fpath, lum);
				if (r != 0) {
					fprintf(stderr, "*** ERROR *** llapi_file_get_stripe of [%s] failed with return value [%d]\n", fpath, r);  //docs say it sets errno, I find that's not the case
					free(lum);
					return 0;
				} else {
					int i, j;
					char match = 0;
					
					//loop over all of this file's stripes
					for (i=0; i<lum->lmm_stripe_count; i++) {
						ostidx = lum->lmm_objects[i].l_ost_idx;
						//check each against list of OSTs to migrate off of
						for (j=0; j<ostsl; j++) {
							if (ostidx==osts[j]) {
								match = 1;
								break;
							}
						}
						if (match) break;
					}

					if (match) {
						if (find_only) {
							fprintf(stdout, "found a file to be migrated: %s\n", fpath);
						} else {
							char *argv[] = {"lfs_migrate", "-y", (char*)NULL, (char*)NULL};
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
	while (1) {
		static struct option longopts[] = {
			{"ost"            , required_argument, NULL, 'o'},
			{"ost-number-base", required_argument, NULL, 'b'},
			{"find-only"      , no_argument      , NULL, 'n'},

			{"help", no_argument, NULL, 'h'},
			{0, 0, 0, 0}
		};
		
		int c = 0;
		int *indexptr = NULL;

		c = getopt_long(argc, argv, "o:b:nh", longopts, indexptr);
		if (c==-1) break;
		switch (c) {
			case 'o': {
				long l = 0;
				char *endptr = NULL;
				
				errno = 0;
				l = strtol(optarg, &endptr, 0);
				
				if (errno!=0 || *endptr!='\0') {
					fprintf(stderr, "*** ERROR *** invalid numerical value [%s] for ost number\n", optarg);
					exit(EXIT_FAILURE);
				}
				if ((l<0 || l>D_LFS_MIGRATE_MAX_OST_COUNT) || (l<INT_MIN || l>INT_MAX)) {
					fprintf(stderr, "*** ERROR *** ost number [%ld] is out of range\n", l);
					exit(EXIT_FAILURE);
				}
				
				osts[ostsl++] = (int)l;
				
				break;
			}
			case 'b':
				ost_number_base = atoi(optarg);
				if (ost_number_base!=10 && ost_number_base!=16) {
					fprintf(stderr, "*** ERROR *** invalid value [%s] for ost number base\n", optarg);
				}
				break;
			case 'n':
				find_only = 1;
				break;
			
			case 'h':
				fputs(helpstr, stdout);
				exit(EXIT_SUCCESS);

			case '?':
				//(getopt_long will have written the error message)
				exit(EXIT_FAILURE);
			default:
				fprintf(stderr, "*** ERROR *** unable to parse command line options\n");
				exit(EXIT_FAILURE);
		}
	}

	//reset argc/argv, forgetting about options above
	argv[optind-1] = argv[0];
	argv += (optind - 1);
	argc -= (optind - 1);

	if (ost_number_base==-1 || ostsl==0 || argc != 2) {
		fprintf(stderr, "usage: d_lfs_migrate --ost-number-base BASE --ost OST_NUMBER... PATH_TO_MIGRATE\n");
		fprintf(stderr, "see  : d_lfs_migrate --help\n");
		fprintf(stderr, "*** ERROR *** invalid usage\n");
		exit(EXIT_FAILURE);
	}


	//---

	fprintf(stdout, "running on top level directory: %s\n", argv[1]);
	dftw(argv[1], lfs_migrate);

	exit(EXIT_SUCCESS);
}

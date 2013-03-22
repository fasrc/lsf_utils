d_lfs_migrate: d_lfs_migrate.c
	$(CC) -g -O2 d_lfs_migrate.c -llustreapi -ldftw -o d_lfs_migrate

test: d_lfs_migrate test_args
	mpirun -np 3 -mca btl tcp,self -mca mpi_warn_on_fork 0 ./d_lfs_migrate $(D_LFS_MIGRATE_TEST_ARGS)
	@echo "now look at *.stdout and *.stderr (e.g.: cat *.stderr && cat *.stdout | grep 'found a file' | sort | md5sum)"

all: d_lfs_migrate

clean:
	rm -f d_lfs_migrate *.stdout *.stderr


test_args:
ifndef D_LFS_MIGRATE_TEST_ARGS
	$(error You must set the D_LFS_MIGRATE_TEST_ARGS environment variable to the path to test)
	#e.g. to just show files to be migrated on OSTs 7, 8, and 9:
	#	export D_LFS_MIGRATE_TEST_ARGS='-n -b10 -o7 -o8 -o9 /SOME/PATH'
endif

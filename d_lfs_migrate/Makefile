d_lfs_migrate: d_lfs_migrate.c
	$(CC) -g -O2 d_lfs_migrate.c -llustreapi -ldftw -o d_lfs_migrate


test_path:
ifndef D_LFS_MIGRATE_TEST_PATH
    $(error You must set the D_LFS_MIGRATE_TEST_PATH environment variable to the path to test)
endif

test: d_lfs_migrate test_path
	mpirun -np 3 -mca btl tcp,self -mca mpi_warn_on_fork 0 ./d_lfs_migrate $(D_LFS_MIGRATE_TEST_PATH)
	@echo "now look at *.stdout and *.stderr (e.g.: cat *.stderr && cat *.stdout | grep 'found a file' | sort | md5sum)"


all: d_lfs_migrate

clean:
	rm -f d_lfs_migrate *.stdout *.stderr

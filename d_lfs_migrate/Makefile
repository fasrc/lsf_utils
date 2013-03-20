d_lfs_migrate: d_lfs_migrate.c
	$(CC) -g -O2 d_lfs_migrate.c -llustreapi -ldftw -o d_lfs_migrate

test: d_lfs_migrate
	mpirun -np 3 -mca btl tcp,self -mca mpi_warn_on_fork 0 ./d_lfs_migrate /n/panlfs/data/MIRROR/ps1-md/gpc1/MD01.20091126/o5098g0184o.100592

all: d_lfs_migrate

clean:
	rm -f d_lfs_migrate 0*.stdout 0*.stderr

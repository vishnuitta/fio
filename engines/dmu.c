#include "../fio.h"
#include "../optgroup.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/dmu.h>
#include <sys/dmu_objset.h>
#include <sys/dmu_tx.h>
#include <sys/dsl_dataset.h>
#include <sys/dsl_pool.h>
#include <sys/dsl_synctask.h>
#include <sys/fs/zfs.h>
#include <sys/vdev.h>
#include <sys/spa.h>
#include <sys/spa_impl.h>
#include <sys/vdev.h>
#include <sys/zap.h>
#include <sys/zfs_context.h>
#include <sys/zfs_znode.h>
#include <sys/zio_checksum.h>
#include <sys/zio_compress.h>
#include <sys/kstat.h>
#include <sys/uzfs_zvol.h>
#include <libzfs.h>
#include <uzfs_io.h>
#include <uzfs_mgmt.h>
#include <zrepl_mgmt.h>

const char *hold_tag = "fio_hold_tag";
#define HOLD_TAG	((void *) hold_tag)

pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;
static int initialized_spa[10] = { 0 };
static int initialized_zv[10] = { 0 };
zvol_state_t *g_zv[10] = { NULL };
spa_t *spa[10] = { NULL } ;
struct dmu_opts {
    void *pad;
    char *pool;
    unsigned int kstats;
};

struct dmu_data {
    int spa_index;
    int os_index;
};
 
static importargs_t g_importargs = {0};
libzfs_handle_t *g_zfs;

#define ZVOL_OBJ 1ULL

static void
fatal(spa_t *spa, void *tag, const char *fmt, ...) {
    va_list ap;

    if (spa != NULL) {
        spa_close(spa, tag);
    }

    va_start(ap, fmt);
    (void)vfprintf(stderr, fmt, ap);
    va_end(ap);
    (void)fprintf(stderr, "\n");

    exit(1);
}

char *pool_dir = "/tmp";

static void
pool_import(char *target, boolean_t readonly) {

	nvlist_t *config;
        nvlist_t *props;
        int error;

	printf("in pool_import\n");
        g_zfs = libzfs_init();
        ASSERT(g_zfs != NULL);

	/* making unique true will require scanning
	 * of the namespace avl
	 */

        g_importargs.unique = B_FALSE;
        g_importargs.can_be_active = B_TRUE;
        g_importargs.scan = B_TRUE;
	g_importargs.path = &pool_dir;
	g_importargs.paths = 1;
//        g_importargs.unique = B_TRUE;
//        g_importargs.scan = B_FALSE;
//	g_importargs.cachefile = NULL;
//	g_importargs.poolname = target;

        error = zpool_tryimport(g_zfs, target, &config, &g_importargs);
        if (error)
                fatal(NULL, FTAG, "cannot import '%s': %s", target,
                    libzfs_error_description(g_zfs));

        props = NULL;
        if (readonly) {
                VERIFY(nvlist_alloc(&props, NV_UNIQUE_NAME, 0) == 0);
                VERIFY(nvlist_add_uint64(props,
                    zpool_prop_to_name(ZPOOL_PROP_READONLY), 1) == 0);
        }

        error = spa_import(target, config, props,
            (readonly ?  ZFS_IMPORT_SKIP_MMP : ZFS_IMPORT_NORMAL));
//        if (error == EEXIST)
//                error = 0;

        if (error)
                fatal(NULL, FTAG, "can't import '%s': %s", target,
                    strerror(error));

}

static spa_t *
user_spa_open(char *target, boolean_t readonly, void *tag) {
    int err;
    spa_t *spa = NULL;

    pool_import(target, readonly);
    printf("pool_import success");
    err = spa_open(target, &spa, tag);
    if (err != 0)
        fatal(NULL, HOLD_TAG, "cannot open '%s': %s", target, strerror(err));
    return spa;
}

static int
fio_dmu_queue(struct thread_data *td, struct io_u *io_u) {
    int error;
    int os_index = (int)((struct dmu_data *)(td->io_ops_data))->os_index;
    zvol_state_t *zv = g_zv[os_index];

    if (io_u->ddir == DDIR_WRITE) {
        error = uzfs_write_data(zv, io_u->xfer_buf, io_u->offset, io_u->xfer_buflen,
	    NULL, B_FALSE);
        if (error != 0) {
            io_u->error = error;
            td_verror(td, io_u->error, "rfer");
        }
        return FIO_Q_COMPLETED;
    } else if (io_u->ddir == DDIR_READ) {
        error = dmu_read(zv->zv_objset, ZVOL_OBJ, io_u->offset, io_u->xfer_buflen,
                         io_u->xfer_buf, DMU_READ_NO_PREFETCH);
        if (error != 0) {
            io_u->error = error;
            td_verror(td, io_u->error, "rfer");
        }
        return FIO_Q_COMPLETED;
    }
    return FIO_Q_COMPLETED;
}

static int
fio_dmu_setup(struct thread_data *td) {
    struct dmu_opts *opts = td->eo;
    char *pooldsname, *dsname, *c;
    int i;
    char poolname[MAXPATHLEN];
    char osname[ZFS_MAX_DATASET_NAME_LEN + 1];
    zvol_state_t *zv;
    static kernel_inited = 0;
    int spa_index;
/*
    if (kernel_inited == 0) {
        printf("uzfs initing..\n");
        uzfs_init();
        kernel_inited = 1;
    }
*/
    pooldsname = opts->pool;
    strncpy(poolname, pooldsname, sizeof(poolname));
    c = strchr(poolname, '/');
    if (c != NULL)
        *c = '\0';
    c++;
    dsname = c;
    for (i=0; i<10 && spa[i] != NULL; i++)
    {
        if(strcmp(spa[i]->spa_name, poolname) == 0)
        {
            initialized_spa[i]++;
            td->io_ops_data = (struct dmu_data *)malloc(sizeof(struct dmu_data));
            ((struct dmu_data *)(td->io_ops_data))->spa_index = i;
	    goto hold_os;
        }
    }

    if (i == 10) {
        fprintf(stderr, "too many pools\n");
        exit(1);
    }

    if (!opts->pool) {
        fprintf(stderr, "No zfs pool name\n");
        exit(1);
    }

    spa[i] = user_spa_open(poolname, B_FALSE, HOLD_TAG);
    printf("spa_open success %s\n", poolname);
    spa[i]->spa_debug = 1;
    initialized_spa[i] = 1;
    td->io_ops_data = (struct dmu_data *)malloc(sizeof(struct dmu_data));
    ((struct dmu_data *)(td->io_ops_data))->spa_index = i;
    spa_index = i;
    c--;
    *c = '/';
hold_os:
    for (i=0; i<10 && g_zv[i] != NULL; i++) {
        if(strcmp(g_zv[i]->zv_name, pooldsname) == 0) {
            initialized_zv[i]++;
            ((struct dmu_data *)(td->io_ops_data))->os_index = i;
	    goto end;
        }
    }

    if (i == 10) {
        fprintf(stderr, "too many datasets\n");
        exit(1);
    }
    zv = NULL;
    uzfs_open_dataset(spa[spa_index], dsname, &zv);
    if (zv == NULL) {
        printf("No zvol info with name %s\n", dsname);
        return 1;
    }
    uzfs_hold_dataset(zv);
    uzfs_update_metadata_granularity(zv, 512);
    g_zv[i] = zv;
    initialized_zv[i] = 1;
    ((struct dmu_data *)(td->io_ops_data))->os_index = i;
end:

    printf("Pool imported %s (job %s)! %d %d %d\n\n\n", dsname, td->o.name,
        i, td->thread_number, td->subjob_number);
    return 0;
}

static void
fio_dmu_cleanup(struct thread_data *td) {
}

static int
fio_dmu_open(struct thread_data *td, struct fio_file *f) {
    /*  we cant open multiple datasets so things are hard coded
     *  the reason that we cant do this is that internally,
     *  threads interacting with the ZFS datastructures are
     *  asserted for certain pthread_t attributes. I really dont want
     *  to change FIO *that* much. Also, its not needed as we
     *  can compare single to single threaded workloads
     */
    return 0;
}

static int
fio_dmu_close(struct thread_data *td, struct fio_file *f) {
    struct dmu_opts *opts = td->eo;
    int spa_index = (int)((struct dmu_data *)(td->io_ops_data))->spa_index;
    int os_index = (int)((struct dmu_data *)(td->io_ops_data))->os_index;

    pthread_mutex_lock(&init_mutex);
    initialized_zv[os_index]--;
    if(initialized_zv[os_index] == 0) {
        if (opts->kstats != 0) {
            kstat_dump_all();
            spa_print_stats(spa[spa_index]);
        }
//        uzfs_zinfo_drop_refcnt(zv[os_index], 0);
    }
    initialized_spa[spa_index]--;
    if(initialized_spa[spa_index] == 0)
        spa_close(spa[spa_index], HOLD_TAG);
    pthread_mutex_unlock(&init_mutex);

    printf("closed %s %d %d\n", td->o.name, td->thread_number, td->subjob_number);
    return 0;
}

struct fio_option options[] = {{
                                   .name = "zvol",
                                   .lname = "zvol to use for IO test",
                                   .type = FIO_OPT_STR_STORE,
                                   .def = NULL,
                                   .help = "selected pool",
                                   .off1 = offsetof(struct dmu_opts, pool),
                                   .category = FIO_OPT_C_ENGINE,
                                   .group = FIO_OPT_G_INVALID,
                               },
                               {
                                   .name = "kstats",
                                   .lname = "Print kstats when jobs finish",
                                   .type = FIO_OPT_BOOL,
                                   .def = "0",
                                   .help = "Print kstats when jobs finish",
                                   .off1 = offsetof(struct dmu_opts, kstats),
                                   .category = FIO_OPT_C_ENGINE,
                                   .group = FIO_OPT_G_INVALID,
                               },
                               {
                                   .name = NULL,
                               }};

struct ioengine_ops ioengine = {
    .name = "dmu",
    .version = FIO_IOOPS_VERSION,
    .setup = fio_dmu_setup,
    .queue = fio_dmu_queue,
    .cleanup = fio_dmu_cleanup,
    .open_file = fio_dmu_open,
    .close_file = fio_dmu_close,
    .options = options,
    .option_struct_size = sizeof(struct dmu_opts),
    .flags = FIO_SYNCIO | FIO_DISKLESSIO | FIO_NOEXTEND | FIO_NODISKUTIL,
};

static void fio_init
fio_dmu_register(void) {
    register_ioengine(&ioengine);
}

static void fio_exit
fio_dmu_unregister(void) {
    unregister_ioengine(&ioengine);
}

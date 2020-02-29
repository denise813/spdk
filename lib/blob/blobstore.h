/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef SPDK_BLOBSTORE_H
#define SPDK_BLOBSTORE_H

#include "spdk/assert.h"
#include "spdk/blob.h"
#include "spdk/queue.h"
#include "spdk/util.h"

#include "request.h"

/* In Memory Data Structures
 *
 * The following data structures exist only in memory.
 */

#define SPDK_BLOB_OPTS_CLUSTER_SZ (1024 * 1024)
#define SPDK_BLOB_OPTS_NUM_MD_PAGES UINT32_MAX
#define SPDK_BLOB_OPTS_MAX_MD_OPS 32
#define SPDK_BLOB_OPTS_DEFAULT_CHANNEL_OPS 512
#define SPDK_BLOB_BLOBID_HIGH_BIT (1ULL << 32)

struct spdk_xattr {
	uint32_t	index;
	uint16_t	value_len;
	char		*name;
	void		*value;
	TAILQ_ENTRY(spdk_xattr)	link;
};

/* The mutable part of the blob data that is sync'd to
 * disk. The data in here is both mutable and persistent.
 */
struct spdk_blob_mut_data {
	/* Number of data clusters in the blob */
	uint64_t	num_clusters;

	/* Array LBAs that are the beginning of a cluster, in
	 * the order they appear in the blob.
	 */
	uint64_t	*clusters;

	/* The size of the clusters array. This is greater than or
	 * equal to 'num_clusters'.
	 */
	size_t		cluster_array_size;

	/* Number of extent pages */
	uint64_t	num_extent_pages;

	/* Array of page offsets into the metadata region,
	 * containing extents. Can contain entries for not yet
	 * allocated pages. */
	uint32_t	*extent_pages;

	/* The size of the extent page array. This is greater than or
	 * equal to 'num_extent_pages'. */
	size_t		extent_pages_array_size;

	/* Number of metadata pages */
	uint32_t	num_pages;

	/* Array of page offsets into the metadata region, in
	 * the order of the metadata page sequence.
	 */
	uint32_t	*pages;
};

enum spdk_blob_state {
	/* The blob in-memory version does not match the on-disk
	 * version.
	 */
	SPDK_BLOB_STATE_DIRTY,

	/* The blob in memory version of the blob matches the on disk
	 * version.
	 */
	SPDK_BLOB_STATE_CLEAN,

	/* The in-memory state being synchronized with the on-disk
	 * blob state. */
	SPDK_BLOB_STATE_LOADING,
};

TAILQ_HEAD(spdk_xattr_tailq, spdk_xattr);

struct spdk_blob_list {
	spdk_blob_id id;
	size_t clone_count;
	TAILQ_HEAD(, spdk_blob_list) clones;
	TAILQ_ENTRY(spdk_blob_list) link;
};

struct spdk_blob {
	/* blobstore的运行时数据结构*/
	struct spdk_blob_store *bs;
	/* 打开引用计数*/
	uint32_t	open_ref;
	/* 
	 * 该blob的id，从该id能够找到blob的首个元数据
	 * 页的索引
	 */
	spdk_blob_id	id;
	/* clone和snapshot使用，记录父blob的id*/
	spdk_blob_id	parent_id;
	/* blob的状态， dirty时表示需要刷新到物理块设备*/
	enum spdk_blob_state		state;

	/* Two copies of the mutable data. One is a version
	 * that matches the last known data on disk (clean).
	 * The other (active) is the current data. Syncing
	 * a blob makes the clean match the active.
	 */
	/*
	 * Clean是底层物理设备上持久化的数据， active是blob
	 * 中新更新过的数据，需要在适当时刻刷到底层物理设备，
	 * 一旦刷到底层设备， active会重新赋值给clean。
	 */
	struct spdk_blob_mut_data	clean;
	struct spdk_blob_mut_data	active;

	bool		invalid;
	bool		data_ro;
	bool		md_ro;
	/* 
	 * blob对应的flags标志位，
	 * 该标志位会持久化到blob的元数据页中
	 */
	uint64_t	invalid_flags;
	uint64_t	data_ro_flags;
	uint64_t	md_ro_flags;
	/* 
	 * 目前只有瘦分配会初始化该字段，
	 * 该虚拟设备用作分配簇时的cow动作
	 */
	struct spdk_bs_dev *back_bs_dev;

	/* TODO: The xattrs are mutable, but we don't want to be
	 * copying them unnecessarily. Figure this out.
	 */
	 /* Blob 的 xattrs 属性 */
	struct spdk_xattr_tailq xattrs;
	/* blob的挂链项*/
	struct spdk_xattr_tailq xattrs_internal;

	TAILQ_ENTRY(spdk_blob) link;

	uint32_t frozen_refcnt;
	bool locked_operation_in_progress;
	enum blob_clear_method clear_method;
	bool extent_rle_found;
	bool extent_table_found;
	bool use_extent_table;

	/* Number of data clusters retrived from extent table,
	 * that many have to be read from extent pages. */
	uint64_t	remaining_clusters_in_et;
};

struct spdk_blob_store {
	/* blob元数据页的起始page偏移*/
	uint64_t			md_start; /* Offset from beginning of disk, in pages */
	/* blob元数据页的page数 */
	uint32_t			md_len; /* Count, in pages */

	struct spdk_io_channel		*md_channel;
	uint32_t			max_channel_ops;
	/* 元数据更新的线程，当前仅支持该线程持久化元数据 */
	struct spdk_thread		*md_thread;
	/* 连接blobstore和bdev的设备对象*/
	struct spdk_bs_dev		*dev;
	/* 三张位图，解释见super_block */
	struct spdk_bit_array		*used_md_pages;
	struct spdk_bit_array		*used_clusters;
	struct spdk_bit_array		*used_blobids;

	pthread_mutex_t			used_clusters_mutex;
	/* 簇大小 */
	uint32_t			cluster_sz;
	/*簇数目*/
	uint64_t			total_clusters;
	/* 数据簇数目*/
	uint64_t			total_data_clusters;
	/*未分配簇数目*/
	uint64_t			num_free_clusters;
	/*一个簇对应的page数目*/
	uint64_t			pages_per_cluster;
	uint32_t			io_unit_size;

	spdk_blob_id			super_blob;
	struct spdk_bs_type		bstype;
	/* unload blobstore后的回调*/
	struct spdk_bs_cpl		unload_cpl;
	int				unload_err;
	/* blobstore中的blobs*/
	TAILQ_HEAD(, spdk_blob)		blobs;
	/*blobstore 中 的 snapshots*/
	TAILQ_HEAD(, spdk_blob_list)	snapshots;

	bool                            clean;
};

struct spdk_bs_channel {
	struct spdk_bs_request_set	*req_mem;
	TAILQ_HEAD(, spdk_bs_request_set) reqs;

	struct spdk_blob_store		*bs;

	struct spdk_bs_dev		*dev;
	struct spdk_io_channel		*dev_channel;

	TAILQ_HEAD(, spdk_bs_request_set) need_cluster_alloc;
	TAILQ_HEAD(, spdk_bs_request_set) queued_io;
};

/** operation type */
enum spdk_blob_op_type {
	SPDK_BLOB_WRITE,
	SPDK_BLOB_READ,
	SPDK_BLOB_UNMAP,
	SPDK_BLOB_WRITE_ZEROES,
	SPDK_BLOB_WRITEV,
	SPDK_BLOB_READV,
};

/* back bs_dev */

#define BLOB_SNAPSHOT "SNAP"
#define SNAPSHOT_IN_PROGRESS "SNAPTMP"
#define SNAPSHOT_PENDING_REMOVAL "SNAPRM"

struct spdk_blob_bs_dev {
	struct spdk_bs_dev bs_dev;
	struct spdk_blob *blob;
};

/* On-Disk Data Structures
 *
 * The following data structures exist on disk.
 */
#define SPDK_BS_INITIAL_VERSION 1
#define SPDK_BS_VERSION 3 /* current version */

#pragma pack(push, 1)

#define SPDK_MD_MASK_TYPE_USED_PAGES 0
#define SPDK_MD_MASK_TYPE_USED_CLUSTERS 1
#define SPDK_MD_MASK_TYPE_USED_BLOBIDS 2

struct spdk_bs_md_mask {
	uint8_t		type;
	uint32_t	length; /* In bits */
	uint8_t		mask[0];
};

#define SPDK_MD_DESCRIPTOR_TYPE_PADDING 0
#define SPDK_MD_DESCRIPTOR_TYPE_XATTR 2
#define SPDK_MD_DESCRIPTOR_TYPE_FLAGS 3
#define SPDK_MD_DESCRIPTOR_TYPE_XATTR_INTERNAL 4

/* Following descriptors define cluster layout in a blob.
 * EXTENT_RLE cannot be present in blobs metadata,
 * at the same time as EXTENT_TABLE and EXTENT_PAGE descriptors. */

/* EXTENT_RLE descriptor holds an array of LBA that points to
 * beginning of allocated clusters. The array is run-length encoded,
 * with 0's being unallocated clusters. It is part of serialized
 * metadata chain for a blob. */
#define SPDK_MD_DESCRIPTOR_TYPE_EXTENT_RLE 1
/* EXTENT_TABLE descriptor holds array of md page offsets that
 * point to pages with EXTENT_PAGE descriptor. The 0's in the array
 * are run-length encoded, non-zero values are unallocated pages.
 * It is part of serialized metadata chain for a blob. */
#define SPDK_MD_DESCRIPTOR_TYPE_EXTENT_TABLE 5
/* EXTENT_PAGE descriptor holds an array of LBAs that point to
 * beginning of allocated clusters. The array is run-length encoded,
 * with 0's being unallocated clusters. It is NOT part of
 * serialized metadata chain for a blob. */
#define SPDK_MD_DESCRIPTOR_TYPE_EXTENT_PAGE 6

/** comment by hy 2020-02-13
 * # blob的属性xattrs
 */
struct spdk_blob_md_descriptor_xattr {
	uint8_t		type;
	uint32_t	length;

	uint16_t	name_length;
	uint16_t	value_length;
	/* 
	 * name 对 应 key ， value 紧 跟 在 name 后 面 ，由
	 * name_length和value_length指定key和value的长度和偏移量。
	 */
	char		name[0];
	/* String name immediately followed by string value. */
};

/** comment by hy 2020-02-13
 * # blob的簇extents
 */
struct spdk_blob_md_descriptor_extent_rle {
	uint8_t		type;
	uint32_t	length;

	struct {
		/* 其实簇的lba地址 */
		uint32_t        cluster_idx;
		/* In units of clusters 该extents对应的簇数目 */
		uint32_t        length; /* In units of clusters */
	} extents[0];
};

struct spdk_blob_md_descriptor_extent_table {
	uint8_t		type;
	uint32_t	length;

	/* Number of data clusters in the blob */
	uint64_t	num_clusters;

	struct {
		uint32_t	page_idx;
		uint32_t	num_pages; /* In units of pages */
	} extent_page[0];
};

struct spdk_blob_md_descriptor_extent_page {
	uint8_t		type;
	uint32_t	length;

	/* First cluster index in this extent page */
	uint32_t	start_cluster_idx;

	uint32_t        cluster_idx[0];
};

#define SPDK_BLOB_THIN_PROV (1ULL << 0)
#define SPDK_BLOB_INTERNAL_XATTR (1ULL << 1)
#define SPDK_BLOB_EXTENT_TABLE (1ULL << 2)
#define SPDK_BLOB_INVALID_FLAGS_MASK	(SPDK_BLOB_THIN_PROV | SPDK_BLOB_INTERNAL_XATTR | SPDK_BLOB_EXTENT_TABLE)

#define SPDK_BLOB_READ_ONLY (1ULL << 0)
#define SPDK_BLOB_DATA_RO_FLAGS_MASK	SPDK_BLOB_READ_ONLY

#define SPDK_BLOB_CLEAR_METHOD_SHIFT 0
#define SPDK_BLOB_CLEAR_METHOD (3ULL << SPDK_BLOB_CLEAR_METHOD_SHIFT)
#define SPDK_BLOB_MD_RO_FLAGS_MASK	SPDK_BLOB_CLEAR_METHOD

/** comment by hy 2020-02-13
 * # blob 的 flags 标志位
 */
struct spdk_blob_md_descriptor_flags {
	uint8_t		type;
	uint32_t	length;

	/*
	 * If a flag in invalid_flags is set that the application is not aware of,
	 *  it will not allow the blob to be opened.
	 */
	 /* blob的三个读写标志位 */
	uint64_t	invalid_flags;

	/*
	 * If a flag in data_ro_flags is set that the application is not aware of,
	 *  allow the blob to be opened in data_read_only and md_read_only mode.
	 */
	uint64_t	data_ro_flags;

	/*
	 * If a flag in md_ro_flags is set the the application is not aware of,
	 *  allow the blob to be opened in md_read_only mode.
	 */
	uint64_t	md_ro_flags;
};

struct spdk_blob_md_descriptor {
	uint8_t		type;
	uint32_t	length;
};

#define SPDK_INVALID_MD_PAGE UINT32_MAX

struct spdk_blob_md_page {
	spdk_blob_id     id;
	/*该blob元数据页的序列号*/
	uint32_t        sequence_num;
	/* 保留字节*/
	uint32_t	reserved0;

	/* Descriptors here */
	/* 
	 * 该数组实际序列化了blob三类元数据：
	 * flags标志位、 blob属性（xattrs）、 blob
	 * 的extents，最终会持久化到物理块设备上。
	*/
	uint8_t		descriptors[4072];

	/* 
	 * 下一个该blob元数据页的page索引号，转换为lba后，
	 * 可以递归读取下一个blob元数据页，例如：
	 * next_page = page->next;
	 * next_lba = _spdk_bs_page_to_lba(blob->bs, 
	 * blob->bs->md_start +next_page);
	*/
	uint32_t	next;
	/* 以上数据的crc校验码 */
	uint32_t	crc;
};
#define SPDK_BS_PAGE_SIZE 0x1000
SPDK_STATIC_ASSERT(SPDK_BS_PAGE_SIZE == sizeof(struct spdk_blob_md_page), "Invalid md page size");

#define SPDK_BS_MAX_DESC_SIZE sizeof(((struct spdk_blob_md_page*)0)->descriptors)

/* Maximum number of extents a single Extent Page can fit.
 * For an SPDK_BS_PAGE_SIZE of 4K SPDK_EXTENTS_PER_EP would be 512. */
#define SPDK_EXTENTS_PER_EP_MAX ((SPDK_BS_MAX_DESC_SIZE - sizeof(struct spdk_blob_md_descriptor_extent_page)) / sizeof(uint32_t))
#define SPDK_EXTENTS_PER_EP (spdk_align64pow2(SPDK_EXTENTS_PER_EP_MAX + 1) >> 1u)

#define SPDK_BS_SUPER_BLOCK_SIG "SPDKBLOB"

struct spdk_bs_super_block {
	/*8个字节的签名，固定为SPDKBLOB */
	uint8_t		signature[8];
	/*blobstore的版本，目前是3 */
	uint32_t        version;
	/* 整个super_blob的长度，目前固定是4KB */
	uint32_t        length;
	uint32_t	clean; /* If there was a clean shutdown, this is 1. */
	/* 
	 * super_blob 对应的id 
	 * 该字段只有正常关闭blobstore时为1，否则为0：
	 * 这意味着每次打开blobstore时，都需要把该字段的物化值更新为0。如果打
	 * 开blobstore时读取该值不为1，则blobstore会进入一种recovery修复模式，
	 * 尝试修复blobstore的不一致数据， 具体见代码实现。
	 */
	spdk_blob_id	super_blob;
	/* 
	 * blobstore的簇大小，创建blobstore时指定，
	 * 不指定默认为4MB
	 */
	uint32_t	cluster_size; /* In bytes */
	/*
	 * 记录blob元数据页分配的位图数组存储在物理块设备上的page偏移量
	 * 由于第 0 个 page 是 super_block，
	 * 那么 used_page_mask_start 恒为 1
	 */
	uint32_t	used_page_mask_start; /* Offset from beginning of disk, in pages */
	/* 
	 * 元数据页分配占用的page数
	 * divide_round_up(sizeof(struct spdk_bs_md_mask) +
	 * divide_round_up(bs->md_len, 8), SPDK_BS_PAGE_SIZE)
	 */
	uint32_t	used_page_mask_len; /* Count, in pages */
	/* 
	 * 记录簇分配的位图数组存储在物理块设备上的page偏移量
	 */

	uint32_t	used_cluster_mask_start; /* Offset from beginning of disk, in pages */
	/* 
	 * 簇分配位数组占用的page数
	 */
	uint32_t	used_cluster_mask_len; /* Count, in pages */
	/* 
	 * blob元数据页在物理块设备上的起始page偏移量
	 * 位图数组一旦分配完，就得到了 blob 元数据页的起始 page
	 */
	uint32_t	md_start; /* Offset from beginning of disk, in pages */
	/* 
	 * blob元数据占用的page数
	 * 元 数 据 使 用 的 簇 数 量 divide_round_up(num_md_pages,
	 * bs->pages_per_cluster) = divide_round_up(381583, 4MB / 4KB)
	 */
	uint32_t	md_len; /* Count, in pages */
	/* blobstore类型 */
	struct spdk_bs_type	bstype; /* blobstore type */
	/* 
	 * 元数据按照占用的 page 数量进行簇整数边界对齐后,剩下的就是数据
	 * 数据簇,创建 blob 时分配给 blob（瘦分配在写入时才进行分配）。
	 * 分配数据簇时要修改位图数组，因此位图数组需要及时刷到物理块设备上
	 */
	uint32_t	used_blobid_mask_start; /* Offset from beginning of disk, in pages */
	/* blobid分配位数组占用的page数 */
	uint32_t	used_blobid_mask_len; /* Count, in pages */

	uint64_t        size; /* size of blobstore in bytes */
	uint32_t        io_unit_size; /* Size of io unit in bytes */
	/* 保留字节 */
	uint8_t         reserved[4000];
	/* crc校验码 */
	uint32_t	crc;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_bs_super_block) == 0x1000, "Invalid super block size");

#pragma pack(pop)

struct spdk_bs_dev *spdk_bs_create_zeroes_dev(void);
struct spdk_bs_dev *spdk_bs_create_blob_bs_dev(struct spdk_blob *blob);

/* Unit Conversions
 *
 * The blobstore works with several different units:
 * - Byte: Self explanatory
 * - LBA: The logical blocks on the backing storage device.
 * - Page: The read/write units of blobs and metadata. This is
 *         an offset into a blob in units of 4KiB.
 * - Cluster Index: The disk is broken into a sequential list of
 *		    clusters. This is the offset from the beginning.
 *
 * NOTE: These conversions all act on simple magnitudes, not with any sort
 *        of knowledge about the blobs themselves. For instance, converting
 *        a page to an lba with the conversion function below simply converts
 *        a number of pages to an equivalent number of lbas, but that
 *        lba certainly isn't the right lba that corresponds to a page offset
 *        for a particular blob.
 */
static inline uint64_t
_spdk_bs_byte_to_lba(struct spdk_blob_store *bs, uint64_t length)
{
	assert(length % bs->dev->blocklen == 0);

	return length / bs->dev->blocklen;
}

static inline uint64_t
_spdk_bs_dev_byte_to_lba(struct spdk_bs_dev *bs_dev, uint64_t length)
{
	assert(length % bs_dev->blocklen == 0);

	return length / bs_dev->blocklen;
}

static inline uint64_t
_spdk_bs_page_to_lba(struct spdk_blob_store *bs, uint64_t page)
{
	return page * SPDK_BS_PAGE_SIZE / bs->dev->blocklen;
}

static inline uint64_t
_spdk_bs_md_page_to_lba(struct spdk_blob_store *bs, uint32_t page)
{
	assert(page < bs->md_len);
	return _spdk_bs_page_to_lba(bs, page + bs->md_start);
}

static inline uint64_t
_spdk_bs_dev_page_to_lba(struct spdk_bs_dev *bs_dev, uint64_t page)
{
	return page * SPDK_BS_PAGE_SIZE / bs_dev->blocklen;
}

static inline uint64_t
_spdk_bs_io_unit_per_page(struct spdk_blob_store *bs)
{
	return SPDK_BS_PAGE_SIZE / bs->io_unit_size;
}

static inline uint64_t
_spdk_bs_io_unit_to_page(struct spdk_blob_store *bs, uint64_t io_unit)
{
	return io_unit / _spdk_bs_io_unit_per_page(bs);
}

static inline uint64_t
_spdk_bs_cluster_to_page(struct spdk_blob_store *bs, uint32_t cluster)
{
	return (uint64_t)cluster * bs->pages_per_cluster;
}

static inline uint32_t
_spdk_bs_page_to_cluster(struct spdk_blob_store *bs, uint64_t page)
{
	assert(page % bs->pages_per_cluster == 0);

	return page / bs->pages_per_cluster;
}

static inline uint64_t
_spdk_bs_cluster_to_lba(struct spdk_blob_store *bs, uint32_t cluster)
{
	return (uint64_t)cluster * (bs->cluster_sz / bs->dev->blocklen);
}

static inline uint32_t
_spdk_bs_lba_to_cluster(struct spdk_blob_store *bs, uint64_t lba)
{
	assert(lba % (bs->cluster_sz / bs->dev->blocklen) == 0);

	return lba / (bs->cluster_sz / bs->dev->blocklen);
}

static inline uint64_t
_spdk_bs_io_unit_to_back_dev_lba(struct spdk_blob *blob, uint64_t io_unit)
{
	return io_unit * (blob->bs->io_unit_size / blob->back_bs_dev->blocklen);
}

static inline uint64_t
_spdk_bs_back_dev_lba_to_io_unit(struct spdk_blob *blob, uint64_t lba)
{
	return lba * (blob->back_bs_dev->blocklen / blob->bs->io_unit_size);
}

static inline uint64_t
_spdk_bs_cluster_to_extent_table_id(uint64_t cluster_num)
{
	return cluster_num / SPDK_EXTENTS_PER_EP;
}

static inline uint32_t *
_spdk_bs_cluster_to_extent_page(struct spdk_blob *blob, uint64_t cluster_num)
{
	uint64_t extent_table_id = _spdk_bs_cluster_to_extent_table_id(cluster_num);

	assert(blob->use_extent_table);
	assert(extent_table_id < blob->active.extent_pages_array_size);

	return &blob->active.extent_pages[extent_table_id];
}

/* End basic conversions */

static inline uint64_t
_spdk_bs_blobid_to_page(spdk_blob_id id)
{
	return id & 0xFFFFFFFF;
}

/* The blob id is a 64 bit number. The lower 32 bits are the page_idx. The upper
 * 32 bits are not currently used. Stick a 1 there just to catch bugs where the
 * code assumes blob id == page_idx.
 */
static inline spdk_blob_id
_spdk_bs_page_to_blobid(uint64_t page_idx)
{
	if (page_idx > UINT32_MAX) {
		return SPDK_BLOBID_INVALID;
	}
	return SPDK_BLOB_BLOBID_HIGH_BIT | page_idx;
}

/* Given an io unit offset into a blob, look up the LBA for the
 * start of that io unit.
 */
static inline uint64_t
_spdk_bs_blob_io_unit_to_lba(struct spdk_blob *blob, uint64_t io_unit)
{
	uint64_t	lba;
	uint64_t	pages_per_cluster;
	uint64_t	io_units_per_cluster;
	uint64_t	io_units_per_page;
	uint64_t	page;

	page = _spdk_bs_io_unit_to_page(blob->bs, io_unit);

	pages_per_cluster = blob->bs->pages_per_cluster;
	io_units_per_page = _spdk_bs_io_unit_per_page(blob->bs);
	io_units_per_cluster = io_units_per_page * pages_per_cluster;

	assert(page < blob->active.num_clusters * pages_per_cluster);

	lba = blob->active.clusters[page / pages_per_cluster];
	lba += io_unit % io_units_per_cluster;
	return lba;
}

/* Given an io_unit offset into a blob, look up the number of io_units until the
 * next cluster boundary.
 */
static inline uint32_t
_spdk_bs_num_io_units_to_cluster_boundary(struct spdk_blob *blob, uint64_t io_unit)
{
	uint64_t	io_units_per_cluster;

	io_units_per_cluster = _spdk_bs_io_unit_per_page(blob->bs) * blob->bs->pages_per_cluster;

	return io_units_per_cluster - (io_unit % io_units_per_cluster);
}

/* Given a page offset into a blob, look up the number of pages until the
 * next cluster boundary.
 */
static inline uint32_t
_spdk_bs_num_pages_to_cluster_boundary(struct spdk_blob *blob, uint64_t page)
{
	uint64_t	pages_per_cluster;

	pages_per_cluster = blob->bs->pages_per_cluster;

	return pages_per_cluster - (page % pages_per_cluster);
}

/* Given an io_unit offset into a blob, look up the number of pages into blob to beginning of current cluster */
static inline uint32_t
_spdk_bs_io_unit_to_cluster_start(struct spdk_blob *blob, uint64_t io_unit)
{
	uint64_t	pages_per_cluster;
	uint64_t	page;

	pages_per_cluster = blob->bs->pages_per_cluster;
	page = _spdk_bs_io_unit_to_page(blob->bs, io_unit);

	return page - (page % pages_per_cluster);
}

/* Given an io_unit offset into a blob, look up the number of pages into blob to beginning of current cluster */
static inline uint32_t
_spdk_bs_io_unit_to_cluster_number(struct spdk_blob *blob, uint64_t io_unit)
{
	return (io_unit / _spdk_bs_io_unit_per_page(blob->bs)) / blob->bs->pages_per_cluster;
}

/* Given an io unit offset into a blob, look up if it is from allocated cluster. */
static inline bool
_spdk_bs_io_unit_is_allocated(struct spdk_blob *blob, uint64_t io_unit)
{
	uint64_t	lba;
	uint64_t	page;
	uint64_t	pages_per_cluster;

	pages_per_cluster = blob->bs->pages_per_cluster;
	page = _spdk_bs_io_unit_to_page(blob->bs, io_unit);

	assert(page < blob->active.num_clusters * pages_per_cluster);

	lba = blob->active.clusters[page / pages_per_cluster];

	if (lba == 0) {
		assert(spdk_blob_is_thin_provisioned(blob));
		return false;
	} else {
		return true;
	}
}

#endif

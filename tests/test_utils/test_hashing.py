# -*- coding: utf-8 -*-
"""Tests for kiara hashing utilities."""

import os
import tempfile

import pytest
from multiformats import CID

from kiara.utils.hashing import (
    KIARA_HASH_FUNCTION,
    NONE_CID,
    compute_cid,
    compute_cid_from_file,
    create_cid_digest,
)


class TestHashingUtilities:
    """Test suite for hashing utility functions."""

    def test_kiara_hash_function(self):
        """Test the KIARA_HASH_FUNCTION constant."""
        # Test that it's the mmh3.hash function
        test_string = "test_data"
        result = KIARA_HASH_FUNCTION(test_string)
        assert isinstance(result, int)
        # Test consistency
        assert KIARA_HASH_FUNCTION(test_string) == KIARA_HASH_FUNCTION(test_string)
        # Test different inputs produce different hashes
        assert KIARA_HASH_FUNCTION("data1") != KIARA_HASH_FUNCTION("data2")

    def test_compute_cid_basic(self):
        """Test basic CID computation."""
        data = {"key": "value"}
        encoded, cid = compute_cid(data)

        assert isinstance(encoded, bytes)
        assert isinstance(cid, CID)
        assert cid.version == 1
        assert cid.codec.name == "dag-cbor"
        assert str(cid).startswith("z")  # base58btc encoding

    def test_compute_cid_consistency(self):
        """Test that same data produces same CID."""
        data = {"test": "data", "number": 42}
        _, cid1 = compute_cid(data)
        _, cid2 = compute_cid(data)

        assert cid1 == cid2
        assert str(cid1) == str(cid2)

    def test_compute_cid_different_data(self):
        """Test that different data produces different CIDs."""
        data1 = {"key": "value1"}
        data2 = {"key": "value2"}

        _, cid1 = compute_cid(data1)
        _, cid2 = compute_cid(data2)

        assert cid1 != cid2
        assert str(cid1) != str(cid2)

    def test_compute_cid_various_data_types(self):
        """Test CID computation with various data types."""
        test_cases = [
            None,
            True,
            False,
            42,
            3.14,
            "string",
            ["list", "of", "items"],
            {"nested": {"dict": "value"}},
            {"mixed": [1, "two", {"three": 3}]},
        ]

        cids = []
        for data in test_cases:
            encoded, cid = compute_cid(data)
            assert isinstance(encoded, bytes)
            assert isinstance(cid, CID)
            cids.append(cid)

        # All CIDs should be unique
        cid_strings = [str(cid) for cid in cids]
        assert len(cid_strings) == len(set(cid_strings))

    def test_compute_cid_with_different_hash_codec(self):
        """Test CID computation with different hash codecs."""
        data = {"test": "data"}

        # Test with sha2-256 (default)
        _, cid_sha256 = compute_cid(data, hash_codec="sha2-256")

        # Test with sha2-512
        _, cid_sha512 = compute_cid(data, hash_codec="sha2-512")

        # CIDs should be different with different hash functions
        assert cid_sha256 != cid_sha512

    def test_compute_cid_with_different_encoding(self):
        """Test CID computation with different encodings."""
        data = {"test": "data"}

        _, cid_base58 = compute_cid(data, encode="base58btc")
        _, cid_base32 = compute_cid(data, encode="base32")

        # Same data but different string representations
        assert str(cid_base58) != str(cid_base32)
        assert str(cid_base58).startswith("z")
        assert str(cid_base32).startswith("b")

    def test_none_cid_constant(self):
        """Test the NONE_CID constant."""
        assert isinstance(NONE_CID, CID)
        # NONE_CID should be consistent
        _, computed_none_cid = compute_cid(None)
        assert NONE_CID == computed_none_cid

    def test_compute_cid_from_file(self):
        """Test CID computation from file."""
        # Create a temporary file with known content
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("This is test file content")
            temp_file = f.name

        try:
            cid = compute_cid_from_file(temp_file)
            assert isinstance(cid, CID)
            assert cid.version == 1
            assert cid.codec.name == "raw"

            # Test consistency
            cid2 = compute_cid_from_file(temp_file)
            assert cid == cid2
        finally:
            os.unlink(temp_file)

    def test_compute_cid_from_file_empty(self):
        """Test CID computation from empty file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_file = f.name

        try:
            cid = compute_cid_from_file(temp_file)
            assert isinstance(cid, CID)
        finally:
            os.unlink(temp_file)

    def test_compute_cid_from_file_large(self):
        """Test CID computation from large file."""
        # Create a file larger than CHUNK_SIZE (65536 bytes)
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
            # Write 100KB of data
            data = b"x" * 100000
            f.write(data)
            temp_file = f.name

        try:
            cid = compute_cid_from_file(temp_file)
            assert isinstance(cid, CID)

            # Verify consistency
            cid2 = compute_cid_from_file(temp_file)
            assert cid == cid2
        finally:
            os.unlink(temp_file)

    def test_compute_cid_from_file_different_codec(self):
        """Test CID computation from file with different codec."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("Test content")
            temp_file = f.name

        try:
            cid_raw = compute_cid_from_file(temp_file, codec="raw")
            cid_dag_pb = compute_cid_from_file(temp_file, codec="dag-pb")

            # Same file content but different codec should produce different CIDs
            assert cid_raw != cid_dag_pb
        finally:
            os.unlink(temp_file)

    def test_compute_cid_from_file_nonexistent(self):
        """Test CID computation from non-existent file."""
        with pytest.raises(FileNotFoundError):
            compute_cid_from_file("/nonexistent/file/path")

    def test_compute_cid_from_file_invalid_hash_codec(self):
        """Test CID computation with unsupported hash codec."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_file = f.name

        try:
            # Currently only sha2-256 is supported
            with pytest.raises(AssertionError):
                compute_cid_from_file(temp_file, hash_codec="sha2-512")
        finally:
            os.unlink(temp_file)

    def test_create_cid_digest_with_string(self):
        """Test creating CID from string digest."""
        # Create a proper multihash digest first
        import hashlib

        from multiformats import multihash

        data = b"test data"
        hash_func = hashlib.sha256
        digest = hash_func(data).digest()
        wrapped = multihash.wrap(digest, "sha2-256")

        cid = create_cid_digest(wrapped)
        assert isinstance(cid, CID)
        assert cid.version == 1
        assert cid.codec.name == "raw"

    def test_create_cid_digest_with_different_codec(self):
        """Test creating CID with different codec."""
        import hashlib

        from multiformats import multihash

        data = b"test data"
        digest = hashlib.sha256(data).digest()
        wrapped = multihash.wrap(digest, "sha2-256")

        cid_raw = create_cid_digest(wrapped, codec="raw")
        cid_dag_cbor = create_cid_digest(wrapped, codec="dag-cbor")

        assert cid_raw.codec.name == "raw"
        assert cid_dag_cbor.codec.name == "dag-cbor"
        assert cid_raw != cid_dag_cbor

    def test_compute_cid_edge_cases(self):
        """Test CID computation with edge cases."""
        # Empty dict
        _, cid_empty_dict = compute_cid({})
        assert isinstance(cid_empty_dict, CID)

        # Empty list
        _, cid_empty_list = compute_cid([])
        assert isinstance(cid_empty_list, CID)

        # Empty string
        _, cid_empty_string = compute_cid("")
        assert isinstance(cid_empty_string, CID)

        # All should produce different CIDs
        assert cid_empty_dict != cid_empty_list
        assert cid_empty_dict != cid_empty_string
        assert cid_empty_list != cid_empty_string

    def test_compute_cid_bytes_handling(self):
        """Test CID computation with bytes data."""
        # Test with bytes
        data = b"binary data"
        encoded, cid = compute_cid(data)
        assert isinstance(cid, CID)

        # Test consistency
        _, cid2 = compute_cid(data)
        assert cid == cid2

    @pytest.mark.parametrize(
        "data,expected_unique",
        [
            ([1, 2, 3], True),
            ({"a": 1, "b": 2}, True),
            ({"nested": {"deep": {"value": 42}}}, True),
            ([{"mixed": "types"}, 123, "string", None], True),
        ],
    )
    def test_compute_cid_deterministic(self, data, expected_unique):
        """Test that CID computation is deterministic."""
        # Compute CID multiple times
        cids = []
        for _ in range(5):
            _, cid = compute_cid(data)
            cids.append(str(cid))

        # All should be the same
        assert len(set(cids)) == 1

    def test_compute_cid_order_independence_for_dicts(self):
        """Test that dict key order doesn't affect CID."""
        # Python 3.7+ maintains dict order, but CBOR should canonicalize
        data1 = {"a": 1, "b": 2, "c": 3}
        data2 = {"c": 3, "a": 1, "b": 2}
        data3 = {"b": 2, "c": 3, "a": 1}

        _, cid1 = compute_cid(data1)
        _, cid2 = compute_cid(data2)
        _, cid3 = compute_cid(data3)

        # All should produce the same CID since the data is logically the same
        assert cid1 == cid2
        assert cid2 == cid3

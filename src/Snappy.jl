module Snappy

using snappy_jll

export compress, uncompress

# snappy status
const SnappyOK             = Cint(0)
const SnappyInvalidInput   = Cint(1)
const SnappyBufferTooSmall = Cint(2)

# High-level Interfaces

"""
    compress(input::AbstractVector{UInt8})
    compress(str::AbstractString)

Compress the input data using the snappy compression algorithm.

Note that providing types other than `Vector{UInt8}` will usually result
in copying.
"""
function compress(input::Vector{UInt8})
    ilen = length(input)
    maxlen = snappy_max_compressed_length(UInt(ilen))
    compressed = Array{UInt8}(undef, maxlen)
    olen, st = snappy_compress(input, compressed)
    if st != SnappyOK
        error("snappy failed to compress; error code $st")
    end
    resize!(compressed, olen)
    compressed
end
compress(input::AbstractVector{UInt8}) = compress(convert(Vector{UInt8}, input))
compress(str::AbstractString) = compress(codeunits(str))

"""
    uncompress(input::AbstractArray{UInt8})
    uncompress(str::AbstractString)

Uncompress the input data using the snappy decompression algorithm.

Note that providing types other than `Array{UInt8}` will usually result
in copying.
"""
function uncompress(input::Array{UInt8})
    ilen = length(input)
    explen, st = snappy_uncompressed_length(input)
    if st != SnappyOK
        error("snappy failed to guess the length of the uncompressed data; error code $st")
    end
    uncompressed = Array{UInt8}(undef, explen)
    olen, st = snappy_uncompress(input, uncompressed)
    if st != SnappyOK
        error("snappy failed to uncompress the data; error code $st")
    end
    @assert explen == olen
    resize!(uncompressed, olen)
    uncompressed
end
uncompress(input::AbstractArray{UInt8}) = uncompress(convert(Array{UInt8}, input))
uncompress(str::AbstractString) = uncompress(codeunits(str))

# Low-level Interfaces

function snappy_compress(input::Vector{UInt8}, compressed::Vector{UInt8})
    ilen = length(input)
    olen = Ref{Csize_t}(length(compressed))
    status = ccall(
        (:snappy_compress, libsnappy),
        Cint,
        (Ptr{UInt8}, Csize_t, Ptr{UInt8}, Ref{Csize_t}),
        input, ilen, compressed, olen
    )
    olen[], status
end

function snappy_uncompress(compressed::Vector{UInt8}, uncompressed::Vector{UInt8})
    ilen = length(compressed)
    olen = Ref{Csize_t}(length(uncompressed))
    status = ccall(
        (:snappy_uncompress, libsnappy),
        Cint,
        (Ptr{UInt8}, Csize_t, Ptr{UInt8}, Ref{Csize_t}),
        compressed, ilen, uncompressed, olen
    )
    olen[], status
end

function snappy_max_compressed_length(source_length::UInt)
    ccall((:snappy_max_compressed_length, libsnappy), Csize_t, (Csize_t,), source_length)
end

function snappy_uncompressed_length(compressed::Vector{UInt8})
    len = length(compressed)
    result = Ref{Csize_t}(0)
    status = ccall((:snappy_uncompressed_length, libsnappy), Cint, (Ptr{UInt8}, Csize_t, Ref{Csize_t}), compressed, len, result)
    result[], status
end

function snappy_validate_compressed_buffer(compressed::Vector{UInt8})
    ilen = length(compressed)
    ccall((:snappy_validate_compressed_buffer, libsnappy), Cint, (Ptr{UInt8}, Csize_t), compressed, ilen)
end

end # module

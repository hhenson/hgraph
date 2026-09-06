#include "descriptor/sha256.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>

namespace hgl::descriptor::detail
{
    namespace
    {
        constexpr std::array<std::uint32_t, 64> round_constants{
            0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
            0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u, 0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
            0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu, 0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
            0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
            0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u, 0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
            0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u, 0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
            0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
            0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u, 0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u,
        };

        [[nodiscard]] constexpr std::uint32_t rotate_right(std::uint32_t value, unsigned bits) noexcept {
            return (value >> bits) | (value << (32U - bits));
        }

        class Sha256
        {
          public:
            Sha256() noexcept
                : state_{0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au, 0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u} {}

            void update(std::span<const std::byte> data) noexcept {
                const auto *bytes     = reinterpret_cast<const std::uint8_t *>(data.data());
                std::size_t remaining = data.size();
                total_bytes_ += remaining;

                if (buffered_ != 0U) {
                    const std::size_t take = std::min(remaining, buffer_.size() - buffered_);
                    std::memcpy(buffer_.data() + buffered_, bytes, take);
                    buffered_ += take;
                    bytes += take;
                    remaining -= take;
                    if (buffered_ == buffer_.size()) {
                        process_block(buffer_.data());
                        buffered_ = 0U;
                    }
                }
                while (remaining >= buffer_.size()) {
                    process_block(bytes);
                    bytes += buffer_.size();
                    remaining -= buffer_.size();
                }
                if (remaining != 0U) {
                    std::memcpy(buffer_.data(), bytes, remaining);
                    buffered_ = remaining;
                }
            }

            [[nodiscard]] std::array<std::byte, 32> finish() noexcept {
                const std::uint64_t bit_length = total_bytes_ * 8U;
                const std::uint8_t  pad_one    = 0x80U;
                update(std::span{reinterpret_cast<const std::byte *>(&pad_one), 1U});
                const std::uint8_t zero = 0U;
                while (buffered_ != 56U) { update(std::span{reinterpret_cast<const std::byte *>(&zero), 1U}); }
                std::array<std::uint8_t, 8> length_be{};
                for (std::size_t index = 0; index < length_be.size(); ++index) {
                    length_be[index] = static_cast<std::uint8_t>(bit_length >> (56U - 8U * index));
                }
                update(std::as_bytes(std::span{length_be}));

                std::array<std::byte, 32> digest{};
                for (std::size_t index = 0; index < state_.size(); ++index) {
                    digest[index * 4U]      = static_cast<std::byte>(state_[index] >> 24U);
                    digest[index * 4U + 1U] = static_cast<std::byte>(state_[index] >> 16U);
                    digest[index * 4U + 2U] = static_cast<std::byte>(state_[index] >> 8U);
                    digest[index * 4U + 3U] = static_cast<std::byte>(state_[index]);
                }
                return digest;
            }

          private:
            void process_block(const std::uint8_t *block) noexcept {
                std::array<std::uint32_t, 64> words{};
                for (std::size_t index = 0; index < 16U; ++index) {
                    words[index] = (static_cast<std::uint32_t>(block[index * 4U]) << 24U) |
                                   (static_cast<std::uint32_t>(block[index * 4U + 1U]) << 16U) |
                                   (static_cast<std::uint32_t>(block[index * 4U + 2U]) << 8U) |
                                   static_cast<std::uint32_t>(block[index * 4U + 3U]);
                }
                for (std::size_t index = 16U; index < words.size(); ++index) {
                    const std::uint32_t s0 =
                        rotate_right(words[index - 15U], 7U) ^ rotate_right(words[index - 15U], 18U) ^ (words[index - 15U] >> 3U);
                    const std::uint32_t s1 =
                        rotate_right(words[index - 2U], 17U) ^ rotate_right(words[index - 2U], 19U) ^ (words[index - 2U] >> 10U);
                    words[index] = words[index - 16U] + s0 + words[index - 7U] + s1;
                }

                std::uint32_t a = state_[0];
                std::uint32_t b = state_[1];
                std::uint32_t c = state_[2];
                std::uint32_t d = state_[3];
                std::uint32_t e = state_[4];
                std::uint32_t f = state_[5];
                std::uint32_t g = state_[6];
                std::uint32_t h = state_[7];
                for (std::size_t index = 0; index < words.size(); ++index) {
                    const std::uint32_t s1       = rotate_right(e, 6U) ^ rotate_right(e, 11U) ^ rotate_right(e, 25U);
                    const std::uint32_t choice   = (e & f) ^ (~e & g);
                    const std::uint32_t temp1    = h + s1 + choice + round_constants[index] + words[index];
                    const std::uint32_t s0       = rotate_right(a, 2U) ^ rotate_right(a, 13U) ^ rotate_right(a, 22U);
                    const std::uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
                    const std::uint32_t temp2    = s0 + majority;
                    h                            = g;
                    g                            = f;
                    f                            = e;
                    e                            = d + temp1;
                    d                            = c;
                    c                            = b;
                    b                            = a;
                    a                            = temp1 + temp2;
                }
                state_[0] += a;
                state_[1] += b;
                state_[2] += c;
                state_[3] += d;
                state_[4] += e;
                state_[5] += f;
                state_[6] += g;
                state_[7] += h;
            }

            std::array<std::uint32_t, 8> state_{};
            std::array<std::uint8_t, 64> buffer_{};
            std::uint64_t                total_bytes_{0U};
            std::size_t                  buffered_{0U};
        };
    }  // namespace

    std::array<char, 64> sha256_hex(std::span<const std::byte> bytes) noexcept {
        Sha256 hasher;
        hasher.update(bytes);
        const std::array<std::byte, 32> digest     = hasher.finish();
        constexpr char                  alphabet[] = "0123456789abcdef";
        std::array<char, 64>            result{};
        for (std::size_t index = 0; index < digest.size(); ++index) {
            const auto value        = static_cast<std::uint8_t>(digest[index]);
            result[index * 2U]      = alphabet[value >> 4U];
            result[index * 2U + 1U] = alphabet[value & 0x0fU];
        }
        return result;
    }
}  // namespace hgl::descriptor::detail

#ifndef HGRAPH_TYPES_UTILS_SLOT_BITMAP_H
#define HGRAPH_TYPES_UTILS_SLOT_BITMAP_H

#include <hgraph/types/storage_metrics.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace hgraph
{
    /**
     * Compact slot-state bitmap with a debugger-readable data layout.
     *
     * The public words/count fields are the stable inspection contract. The
     * mutating surface intentionally matches the small subset of
     * ``dynamic_bitset`` used by the slot stores.
     */
    struct SlotBitmap
    {
        static constexpr std::size_t bits_per_word = sizeof(std::uint64_t) * 8U;

        std::uint64_t *words{nullptr};
        std::size_t bit_count{0};
        std::size_t word_capacity{0};

        SlotBitmap() noexcept = default;
        SlotBitmap(const SlotBitmap &) = delete;
        SlotBitmap &operator=(const SlotBitmap &) = delete;

        SlotBitmap(SlotBitmap &&other) noexcept
            : words(std::exchange(other.words, nullptr)), bit_count(std::exchange(other.bit_count, 0)),
              word_capacity(std::exchange(other.word_capacity, 0))
        {
        }

        SlotBitmap &operator=(SlotBitmap &&other) noexcept
        {
            if (this != &other)
            {
                delete[] words;
                words = std::exchange(other.words, nullptr);
                bit_count = std::exchange(other.bit_count, 0);
                word_capacity = std::exchange(other.word_capacity, 0);
            }
            return *this;
        }

        ~SlotBitmap()
        {
            delete[] words;
        }

        [[nodiscard]] std::size_t size() const noexcept
        {
            return bit_count;
        }
        [[nodiscard]] std::size_t word_count() const noexcept
        {
            return words_for(bit_count);
        }

        void resize(std::size_t new_size)
        {
            const std::size_t old_word_count = word_count();
            const std::size_t required_words = words_for(new_size);
            if (required_words > word_capacity)
            {
                auto replacement = std::make_unique<std::uint64_t[]>(required_words);
                const std::size_t preserved_words = std::min(old_word_count, word_capacity);
                for (std::size_t index = 0; index < preserved_words; ++index)
                {
                    replacement[index] = words[index];
                }
                delete[] words;
                words = replacement.release();
                word_capacity = required_words;
            }
            else if (required_words > old_word_count)
            {
                std::fill(words + old_word_count, words + required_words, std::uint64_t{0});
            }

            bit_count = new_size;
            clear_unused_bits();
        }

        void clear() noexcept
        {
            delete[] words;
            words = nullptr;
            bit_count = 0;
            word_capacity = 0;
        }

        void reset() noexcept
        {
            if (word_count() != 0)
            {
                std::fill_n(words, word_count(), std::uint64_t{0});
            }
        }

        void reset(std::size_t bit) noexcept
        {
            if (bit < bit_count)
            {
                words[word_index(bit)] &= ~bit_mask(bit);
            }
        }

        void set(std::size_t bit) noexcept
        {
            if (bit < bit_count)
            {
                words[word_index(bit)] |= bit_mask(bit);
            }
        }

        [[nodiscard]] bool test(std::size_t bit) const noexcept
        {
            return bit < bit_count && (words[word_index(bit)] & bit_mask(bit)) != 0;
        }

        [[nodiscard]] bool any() const noexcept
        {
            for (std::size_t index = 0; index < word_count(); ++index)
            {
                if (words[index] != 0)
                {
                    return true;
                }
            }
            return false;
        }

        [[nodiscard]] std::size_t count() const noexcept
        {
            std::size_t result = 0;
            for (std::size_t index = 0; index < word_count(); ++index)
            {
                result += static_cast<std::size_t>(std::popcount(words[index]));
            }
            return result;
        }

        /** Exact occupied/retained heap bytes for the bitmap words. */
        [[nodiscard]] DynamicStorageMetrics dynamic_storage_metrics() const noexcept
        {
            return {
                .live_bytes = word_count() * sizeof(std::uint64_t),
                .reserved_bytes = word_capacity * sizeof(std::uint64_t),
            };
        }

      private:
        [[nodiscard]] static constexpr std::size_t words_for(std::size_t bits) noexcept
        {
            return (bits + bits_per_word - 1U) / bits_per_word;
        }

        [[nodiscard]] static constexpr std::size_t word_index(std::size_t bit) noexcept
        {
            return bit / bits_per_word;
        }

        [[nodiscard]] static constexpr std::uint64_t bit_mask(std::size_t bit) noexcept
        {
            return std::uint64_t{1} << (bit % bits_per_word);
        }

        void clear_unused_bits() noexcept
        {
            const std::size_t used_words = word_count();
            if (used_words == 0)
            {
                return;
            }
            const std::size_t remainder = bit_count % bits_per_word;
            if (remainder != 0)
            {
                words[used_words - 1] &= (std::uint64_t{1} << remainder) - 1U;
            }
        }
    };

    static_assert(sizeof(SlotBitmap) == 3 * sizeof(void *));
} // namespace hgraph

#endif // HGRAPH_TYPES_UTILS_SLOT_BITMAP_H

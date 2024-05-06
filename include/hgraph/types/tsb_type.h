//
// Created by Howard Henson on 05/05/2024.
//

#ifndef TSB_TYPE_H
#define TSB_TYPE_H

#include<hgraph/types/time_series_type.h>
#include<vector>
#include<algorithm>
#include <ranges>

namespace hgraph {

    template<typename TIME_SERIES_TYPE>
    struct TimeSeriesBundle {
        explicit TimeSeriesBundle(const std::vector<std::string>& keys): _ts_keys{keys}, _ts_values{} {
            _ts_values.reserve(_ts_keys.size());
        }

        [[nodiscard]] TIME_SERIES_TYPE *get(const std::string& attr) const {
            auto it{std::find(_ts_keys.begin(), _ts_keys.end(), attr)};
            if(it != _ts_keys.end()) {
                size_t index = std::distance(_ts_keys.begin(), it);
                return _ts_values[index];
            }
            return nullptr;
        }

        [[nodiscard]] TIME_SERIES_TYPE *get(size_t attr) const {
            if(attr >= _ts_values.size()) {
                return nullptr;
            }
            return _ts_values[attr];
        }

        [[nodiscard]] auto keys() const { return _ts_keys; }
        [[nodiscard]] auto values() const {return _ts_values; }
        [[nodiscard]] auto items() const {
            return std::ranges::views::zip(_ts_keys, _ts_values);
        }

    private:
        const std::vector<std::string>& _ts_keys;
        std::vector<TIME_SERIES_TYPE *> _ts_values;
    };

    struct TimeSeriesBundleInput : TimeSeriesInput, TimeSeriesBundle<TimeSeriesInput> {

    };
}

#endif //TSB_TYPE_H

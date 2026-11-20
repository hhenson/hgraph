#include <hgraph/types/primitive_types.h>
#include <hgraph/types/type_carrier.h>
#ifndef HGRAPH_NO_ARROW
#include <hgraph/types/frame.h>
#include <hgraph/types/series.h>
#endif

#include <sstream>

int main()
{
    std::ostringstream output;
    output << hgraph::bytes_("a\x01") << ' ' << hgraph::Time{} << ' ' << hgraph::TypeCarrier::of_size(3);
    if (output.str() != "b'a\\x01' 00:00:00 Size[3]") { return 1; }

#ifndef HGRAPH_NO_ARROW
    const hgraph::Series series{};
    output.str("");
    output << hgraph::Frame{} << ' ' << series;
    if (output.str() != "frame[empty] series[empty]" || hgraph::series_length(series) != 0) { return 2; }
#endif
    return 0;
}

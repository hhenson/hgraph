#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/initialize.h>
#include <arrow/util/config.h>

#include <iostream>

int main()
{
    // Check before constructing a scalar: mismatched Arrow 25/26 headers and
    // libraries can otherwise crash when compute calls its virtual data().
    if (arrow::GetBuildInfo().version_major != ARROW_VERSION_MAJOR)
    {
        std::cerr << "Arrow headers " << ARROW_VERSION_MAJOR << " do not match runtime "
                  << arrow::GetBuildInfo().version_major << '\n';
        return 1;
    }
    if (!arrow::compute::Initialize().ok()) { return 2; }
    arrow::Int64Builder builder;
    if (!builder.AppendValues({1, 2, 3}).ok()) { return 3; }
    auto array = builder.Finish();
    if (!array.ok()) { return 4; }
    auto result = arrow::compute::CallFunction("add", {*array, arrow::MakeScalar(0.5)});
    if (!result.ok() || !result->is_array()) { return 5; }
    auto values = std::dynamic_pointer_cast<arrow::DoubleArray>(result->make_array());
    if (!values || values->length() != 3) { return 6; }
    for (int index = 0; index < 3; ++index)
    {
        if (values->Value(index) != index + 1.5) { return 7; }
    }
    return 0;
}

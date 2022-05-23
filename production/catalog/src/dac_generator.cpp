////////////////////////////////////////////////////
// Copyright (c) Gaia Platform LLC
//
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE.txt file
// or at https://opensource.org/licenses/MIT.
////////////////////////////////////////////////////

#include "dac_generator.hpp"

#include <string>

#include <flatbuffers/code_generators.h>

#include "gaia_internal/catalog/catalog.hpp"
#include "gaia_internal/catalog/catalog_facade.hpp"
#include "gaia_internal/gaiac/catalog_facade.hpp"

namespace gaia
{
namespace catalog
{
namespace generate
{

const std::string c_indentation_string("    ");

std::string dac_compilation_unit_writer_t::write_header()
{
    flatbuffers::CodeWriter code(c_indentation_string);
    code += generate_copyright();
    code += generate_open_header_guard();
    code += generate_includes();
    code += generate_open_namespace();
    code += generate_constants();
    code += generate_forward_declarations();
    code += generate_ref_forward_declarations();

    for (const table_facade_t& table : m_database.tables())
    {
        class_writer_t class_writer{table};
        code += class_writer.write_header();
    }

    code += generate_close_namespace();
    code += generate_close_header_guard();
    return code.ToString();
}

std::string dac_compilation_unit_writer_t::write_cpp()
{
    flatbuffers::CodeWriter code(c_indentation_string);
    code += generate_copyright();
    code += generate_includes_cpp();
    code += generate_open_namespace();

    for (const table_facade_t& table : m_database.tables())
    {
        class_writer_t class_writer{table};
        code += class_writer.write_cpp();
    }

    code += generate_close_namespace();
    return code.ToString();
}
flatbuffers::CodeWriter dac_compilation_unit_writer_t::create_code_writer()
{
    flatbuffers::CodeWriter code(c_indentation_string);
    code.SetValue("DBNAME", m_database.database_name());
    return code;
}

std::string dac_compilation_unit_writer_t::generate_copyright()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "////////////////////////////////////////////////////";
    code += "// Copyright (c) Gaia Platform LLC";
    code += "//";
    code += "// Use of this source code is governed by the MIT";
    code += "// license that can be found in the LICENSE.txt file";
    code += "// or at https://opensource.org/licenses/MIT.";
    code += "////////////////////////////////////////////////////";
    code += "";
    code += "// Automatically generated by the Gaia Direct Access Classes code generator.";
    code += "// Do not modify.";

    return code.ToString();
}

std::string dac_compilation_unit_writer_t::generate_open_header_guard()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "#ifndef GAIA_GENERATED_{{DBNAME}}_H_";
    code += "#define GAIA_GENERATED_{{DBNAME}}_H_";

    return code.ToString();
}

std::string dac_compilation_unit_writer_t::generate_close_header_guard()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "#endif  // GAIA_GENERATED_{{DBNAME}}_H_";
    return code.ToString();
}

std::string dac_compilation_unit_writer_t::generate_includes()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "#include <gaia/optional.hpp>";
    code += "#include <gaia/direct_access/dac_object.hpp>";
    code += "#include <gaia/direct_access/dac_iterators.hpp>";
    code += "#include \"{{DBNAME}}_generated.h\"";

    return code.ToString();
}

std::string dac_compilation_unit_writer_t::generate_includes_cpp()
{
    return "#include \"{GENERATED_DAC_HEADER}\"\n";
}

std::string dac_compilation_unit_writer_t::generate_open_namespace()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "namespace " + c_gaia_namespace;
    code += "{";
    if (!m_database.is_default_database())
    {
        code += "namespace {{DBNAME}}";
        code += "{";
    }

    return code.ToString();
}

std::string dac_compilation_unit_writer_t::generate_close_namespace()
{
    flatbuffers::CodeWriter code = create_code_writer();
    if (!m_database.is_default_database())
    {
        code += "}  // namespace {{DBNAME}}";
    }
    code += "}  // namespace " + c_gaia_namespace;

    return code.ToString();
}

std::string dac_compilation_unit_writer_t::generate_constants()
{
    flatbuffers::CodeWriter code = create_code_writer();
    // A fixed constant is used for the flatbuffer builder constructor.
    code += "// The initial size of the flatbuffer builder buffer.";
    code += "constexpr size_t c_flatbuffer_builder_size = 128;";
    code += "";

    for (const table_facade_t& table : m_database.tables())
    {
        code.SetValue("TABLE_NAME", table.table_name());
        code.SetValue("TABLE_TYPE", table.table_type());
        code += "// Constants contained in the {{TABLE_NAME}} object.";
        code += "constexpr common::gaia_type_t::value_type c_gaia_type_{{TABLE_NAME}} = {{TABLE_TYPE}}u;";

        // This map is used to place the constants ordered by offset.
        // There is no practical reason besides making the code easier to read.
        // The '//' in the end of each line prevents the new line to be created.
        std::map<uint16_t, std::string> table_constants;

        for (const auto& incoming_link : table.incoming_links())
        {
            gaiac::incoming_link_facade_t link{incoming_link};
            flatbuffers::CodeWriter const_code = create_code_writer();
            const_code.SetValue("PARENT_OFFSET", link.parent_offset());
            const_code.SetValue("PARENT_OFFSET_VALUE", std::to_string(link.parent_offset_value()));
            const_code += "constexpr common::reference_offset_t {{PARENT_OFFSET}} = {{PARENT_OFFSET_VALUE}};\\";
            table_constants.insert({link.parent_offset_value(), const_code.ToString()});

            const_code.Clear();
            const_code.SetValue("NEXT_OFFSET", link.next_offset());
            const_code.SetValue("NEXT_OFFSET_VALUE", std::to_string(link.next_offset_value()));
            const_code += "constexpr common::reference_offset_t {{NEXT_OFFSET}} = {{NEXT_OFFSET_VALUE}};\\";
            table_constants.insert({link.next_offset_value(), const_code.ToString()});
        }

        for (const auto& outgoing_link : table.outgoing_links())
        {
            gaiac::outgoing_link_facade_t link{outgoing_link};
            flatbuffers::CodeWriter const_code = create_code_writer();
            const_code.SetValue("FIRST_OFFSET", link.first_offset());
            const_code.SetValue("FIRST_OFFSET_VALUE", std::to_string(link.first_offset_value()));
            const_code += "constexpr common::reference_offset_t {{FIRST_OFFSET}} = {{FIRST_OFFSET_VALUE}};\\";
            table_constants.insert({link.first_offset_value(), const_code.ToString()});
        }

        for (auto& constant_pair : table_constants)
        {
            code += constant_pair.second;
        }
        code += "";
    }

    return code.ToString();
}

std::string dac_compilation_unit_writer_t::generate_forward_declarations()
{
    flatbuffers::CodeWriter code = create_code_writer();

    for (const auto& table : m_database.tables())
    {
        code.SetValue("CLASS_NAME", table.class_name());
        code += "class {{CLASS_NAME}};";
    }
    std::string str = code.ToString();
    return str;
}

std::string dac_compilation_unit_writer_t::generate_ref_forward_declarations()
{
    flatbuffers::CodeWriter code = create_code_writer();

    for (const auto& table : m_database.tables())
    {
        if (table.needs_reference_class())
        {
            code.SetValue("TABLE_NAME", table.table_name());
            code += "class {{TABLE_NAME}}_ref_t;";
        }
    }

    std::string str = code.ToString();
    return str;
}

std::string class_writer_t::write_header()
{
    flatbuffers::CodeWriter code(c_indentation_string);
    code += generate_writer() + "\\";
    code += generate_class_definition() + "\\";
    increment_indent();
    code += generate_friend_declarations() + "\\";
    decrement_indent();
    code += "public:";
    increment_indent();
    code += generate_list_types() + "\\";
    code += generate_public_constructor() + "\\";
    code += generate_gaia_typename_accessor() + "\\";
    code += generate_gaia_table_hash_accessor() + "\\";
    code += generate_insert() + "\\";
    code += generate_list_accessor() + "\\";
    code += generate_fields_accessors() + "\\";
    code += generate_incoming_links_accessors() + "\\";
    code += generate_outgoing_links_accessors();
    code += generate_expressions() + "\\";
    decrement_indent();
    code += "private:";
    increment_indent();
    code += generate_private_constructor() + "\\";
    decrement_indent();
    code += generate_close_class_definition();
    code += generate_ref_class();
    code += generate_expr_namespace();
    code += generate_expr_instantiation_cpp();
    return code.ToString();
}

std::string class_writer_t::write_cpp()
{
    flatbuffers::CodeWriter code(c_indentation_string);
    code += generate_class_section_comment_cpp();
    code += generate_gaia_typename_accessor_cpp() + "\\";
    code += generate_gaia_table_hash_accessor_cpp() + "\\";
    code += generate_insert_cpp() + "\\";
    code += generate_list_accessor_cpp() + "\\";
    code += generate_fields_accessors_cpp() + "\\";
    code += generate_incoming_links_accessors_cpp() + "\\";
    code += generate_outgoing_links_accessors_cpp() + "\\";
    code += generate_ref_class_cpp() + "\\";
    return code.ToString();
}

flatbuffers::CodeWriter class_writer_t::create_code_writer()
{
    flatbuffers::CodeWriter code(c_indentation_string);
    code.SetValue("TABLE_NAME", m_table.table_name());
    code.SetValue("CLASS_NAME", m_table.class_name());
    code.SetValue("CLASS_HASH", m_table.hash());

    for (size_t i = 0; i < m_indent_level; i++)
    {
        code.IncrementIdentLevel();
    }

    return code;
}

std::string class_writer_t::generate_class_section_comment_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "//";
    code += "// Implementation of class {{CLASS_NAME}}.";
    code += "//";
    return code.ToString();
}

std::string class_writer_t::generate_writer()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "typedef gaia::direct_access::dac_writer_t<c_gaia_type_{{TABLE_NAME}}, {{CLASS_NAME}}, internal::{{TABLE_NAME}}, internal::{{TABLE_NAME}}T> "
            "{{TABLE_NAME}}_writer;";
    return code.ToString();
}

std::string class_writer_t::generate_class_definition()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "class {{CLASS_NAME}} : public gaia::direct_access::dac_object_t<c_gaia_type_{{TABLE_NAME}}, {{CLASS_NAME}}, "
            "internal::{{TABLE_NAME}}, internal::{{TABLE_NAME}}T> {";
    return code.ToString();
}

std::string class_writer_t::generate_list_types()
{
    flatbuffers::CodeWriter code = create_code_writer();
    for (link_facade_t& link : m_table.outgoing_links())
    {
        code.SetValue("FIELD_NAME", link.field_name());
        code.SetValue("CHILD_TABLE", link.to_class_name());

        if (link.is_multiple_cardinality())
        {
            if (link.is_value_linked())
            {
                code += "typedef gaia::direct_access::value_linked_reference_container_t<{{CHILD_TABLE}}> "
                        "{{FIELD_NAME}}_list_t;";
            }
            else
            {
                code += "typedef gaia::direct_access::reference_container_t<{{CHILD_TABLE}}> "
                        "{{FIELD_NAME}}_list_t;";
            }
        }
    }
    return code.ToString();
}

std::string class_writer_t::generate_public_constructor()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "{{CLASS_NAME}}() : dac_object_t() {}";
    return code.ToString();
}

std::string class_writer_t::generate_insert()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "static gaia::common::gaia_id_t insert_row(\\";
    code += generate_method_params(m_table.fields()) + "\\";
    code += ");";
    return code.ToString();
}

std::string class_writer_t::generate_insert_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "gaia::common::gaia_id_t {{CLASS_NAME}}::insert_row(\\";
    code += generate_method_params(m_table.fields()) + "\\";
    code += ")";
    code += "{";
    code.IncrementIdentLevel();

    code += "flatbuffers::FlatBufferBuilder b(c_flatbuffer_builder_size);";
    code += "b.ForceDefaults(true);";
    code.SetValue("DIRECT", m_table.has_string_or_vector() ? "Direct" : "");
    code += "b.Finish(internal::Create{{TABLE_NAME}}{{DIRECT}}(b\\";

    for (const auto& field : m_table.fields())
    {
        code += ", \\";
        if (field.is_vector())
        {
            code += "&\\";
        }
        code += field.field_name() + "\\";
    }
    code += "));";
    code += "return dac_object_t::insert_row(b);";
    code.DecrementIdentLevel();
    code += "}";
    code += "";
    return code.ToString();
}

std::string class_writer_t::generate_gaia_typename_accessor()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "static const char* gaia_typename();";
    return code.ToString();
}

std::string class_writer_t::generate_gaia_typename_accessor_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "const char* {{CLASS_NAME}}::gaia_typename()";
    code += "{";
    code.IncrementIdentLevel();
    code += "static const char* gaia_typename = \"{{CLASS_NAME}}\";";
    code += "return gaia_typename;";
    code.DecrementIdentLevel();
    code += "}";
    code += "";
    return code.ToString();
}

std::string class_writer_t::generate_gaia_table_hash_accessor()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "static const char* gaia_hash();";
    return code.ToString();
}

std::string class_writer_t::generate_gaia_table_hash_accessor_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "const char* {{CLASS_NAME}}::gaia_hash()";
    code += "{";
    code.IncrementIdentLevel();
    code += "static const char* gaia_hash = \"{{CLASS_HASH}}\";";
    code += "return gaia_hash;";
    code.DecrementIdentLevel();
    code += "}";
    code += "";
    return code.ToString();
}

std::string class_writer_t::generate_list_accessor()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "static gaia::direct_access::dac_container_t<c_gaia_type_{{TABLE_NAME}}, {{CLASS_NAME}}> list();";
    return code.ToString();
}

std::string class_writer_t::generate_list_accessor_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "gaia::direct_access::dac_container_t<c_gaia_type_{{TABLE_NAME}}, {{CLASS_NAME}}> {{CLASS_NAME}}::list()";
    code += "{";
    code.IncrementIdentLevel();
    code += "return gaia::direct_access::dac_container_t<c_gaia_type_{{TABLE_NAME}}, {{CLASS_NAME}}>();";
    code.DecrementIdentLevel();
    code += "}";
    code += "";
    return code.ToString();
}

std::string class_writer_t::generate_fields_accessors()
{
    flatbuffers::CodeWriter code = create_code_writer();

    for (const auto& field : m_table.fields())
    {
        code.SetValue("TYPE", field.field_type());
        code.SetValue("FIELD_NAME", field.field_name());
        code += "{{TYPE}} {{FIELD_NAME}}() const;";
    }

    return code.ToString();
}

std::string class_writer_t::generate_fields_accessors_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();

    // Below, a flatbuffer method is invoked as Create{{TABLE_NAME}}() or as
    // Create{{TABLE_NAME}}Direct. The choice is determined by whether any of
    // the fields are strings or vectors. If at least one is a string or a
    // vector, than the Direct variation is used.
    for (const auto& field : m_table.fields())
    {
        code.SetValue("TYPE", field.field_type());
        code.SetValue("FIELD_NAME", field.field_name());

        if (field.is_string())
        {
            code.SetValue("FUNCTION_NAME", "GET_STR");
        }
        else if (field.is_vector())
        {
            code.SetValue("FUNCTION_NAME", "GET_ARRAY");
        }
        else
        {
            code.SetValue("FUNCTION_NAME", "GET");
        }
        code += "{{TYPE}} {{CLASS_NAME}}::{{FIELD_NAME}}() const";
        code += "{";
        code.IncrementIdentLevel();
        code += "return {{FUNCTION_NAME}}({{FIELD_NAME}});";
        code.DecrementIdentLevel();
        code += "}";
        code += "";
    }

    return code.ToString();
}

std::string class_writer_t::generate_incoming_links_accessors()
{
    flatbuffers::CodeWriter code = create_code_writer();

    // Iterate over the relationships where the current table is the child
    for (auto& link : m_table.incoming_links())
    {
        code.SetValue("FIELD_NAME", link.field_name());
        code.SetValue("PARENT_CLASS_NAME", link.to_class_name());

        code += "{{PARENT_CLASS_NAME}} {{FIELD_NAME}}() const;";
    }

    return code.ToString();
}

std::string class_writer_t::generate_incoming_links_accessors_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();

    // Iterate over the relationships where the current table is the child
    for (auto& incoming_link : m_table.incoming_links())
    {
        gaiac::incoming_link_facade_t link{incoming_link};
        code.SetValue("FIELD_NAME", link.field_name());
        code.SetValue("PARENT_CLASS_NAME", link.to_class_name());
        code.SetValue("PARENT_OFFSET", link.parent_offset());

        code += "{{PARENT_CLASS_NAME}} {{CLASS_NAME}}::{{FIELD_NAME}}() const";
        code += "{";
        code.IncrementIdentLevel();
        if (incoming_link.is_value_linked())
        {
            code += "gaia::common::gaia_id_t id = dac_db_t::get_reference(this->references()[{{PARENT_OFFSET}}], gaia::common::c_ref_anchor_parent_offset);";
        }
        else
        {
            code += "gaia::common::gaia_id_t anchor_id = this->references()[{{PARENT_OFFSET}}];";
            code += "if (!anchor_id.is_valid())";
            code += "{";
            code.IncrementIdentLevel();
            code += "return {{PARENT_CLASS_NAME}}();";
            code.DecrementIdentLevel();
            code += "}";
            code += "gaia::common::gaia_id_t id = dac_db_t::get_reference(anchor_id, gaia::common::c_ref_anchor_parent_offset);";
        }
        code += "return (id.is_valid()) ? {{PARENT_CLASS_NAME}}::get(id) : {{PARENT_CLASS_NAME}}();";
        code.DecrementIdentLevel();
        code += "}";
        code += "";
    }

    return code.ToString();
}

std::string class_writer_t::generate_outgoing_links_accessors()
{
    flatbuffers::CodeWriter code = create_code_writer();

    // Iterate over the relationships where the current table appear as parent
    for (auto& link : m_table.outgoing_links())
    {
        if (link.is_multiple_cardinality())
        {
            code.SetValue("FIELD_NAME", link.field_name());
            code += "{{FIELD_NAME}}_list_t {{FIELD_NAME}}() const;";
        }
        else if (link.is_single_cardinality())
        {
            code.SetValue("FIELD_NAME", link.field_name());
            code.SetValue("CHILD_TABLE", link.to_table());
            code += "{{CHILD_TABLE}}_ref_t {{FIELD_NAME}}() const; ";
        }
        else
        {
            ASSERT_UNREACHABLE("Unsupported relationship cardinality!");
        }
    }

    return code.ToString();
}

std::string class_writer_t::generate_outgoing_links_accessors_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();

    // Iterate over the relationships where the current table appear as parent
    for (auto& outgoing_link : m_table.outgoing_links())
    {
        gaiac::outgoing_link_facade_t link{outgoing_link};
        code.SetValue("CHILD_TABLE", link.to_table());
        code.SetValue("FIELD_NAME", link.field_name());
        code.SetValue("FIRST_OFFSET", link.first_offset());
        code.SetValue("PARENT_OFFSET", link.parent_offset());
        code.SetValue("NEXT_OFFSET", link.next_offset());

        if (link.is_multiple_cardinality())
        {
            if (link.is_value_linked())
            {
                code += "{{CLASS_NAME}}::{{FIELD_NAME}}_list_t {{CLASS_NAME}}::{{FIELD_NAME}}() const";
                code += "{";
                code.IncrementIdentLevel();
                code += "return {{CLASS_NAME}}::{{FIELD_NAME}}_list_t(this->references()[{{FIRST_OFFSET}}], {{PARENT_OFFSET}});";
                code.DecrementIdentLevel();
                code += "}";
            }
            else
            {
                code += "{{CLASS_NAME}}::{{FIELD_NAME}}_list_t {{CLASS_NAME}}::{{FIELD_NAME}}() const";
                code += "{";
                code.IncrementIdentLevel();
                code += "return {{CLASS_NAME}}::{{FIELD_NAME}}_list_t(gaia_id(), {{FIRST_OFFSET}}, {{NEXT_OFFSET}});";
                code.DecrementIdentLevel();
                code += "}";
            }
        }
        else if (link.is_single_cardinality())
        {
            code += "{{CHILD_TABLE}}_ref_t {{CLASS_NAME}}::{{FIELD_NAME}}() const";
            code += "{";
            code.IncrementIdentLevel();
            code += "gaia::common::gaia_id_t anchor_id = this->references()[{{FIRST_OFFSET}}];";
            code += "gaia::common::gaia_id_t child_id = (anchor_id.is_valid()) ? dac_db_t::get_reference(anchor_id, gaia::common::c_ref_anchor_first_child_offset) : gaia::common::c_invalid_gaia_id;";
            code += "return {{CHILD_TABLE}}_ref_t(gaia_id(), child_id, {{FIRST_OFFSET}});";
            code.DecrementIdentLevel();
            code += "}";
        }
        else
        {
            ASSERT_UNREACHABLE("Unsupported relationship cardinality!");
        }
        code += "";
    }

    return code.ToString();
}

std::string class_writer_t::generate_expressions()
{
    flatbuffers::CodeWriter code = create_code_writer();

    // Add DAC expressions and use a dummy template to emulate C++17 inline variable
    // declarations with C++11 legal syntax.
    code += "template<class unused_t>";
    code += "struct expr_ {";
    code.IncrementIdentLevel();

    std::pair<std::string, std::string> expr_variable;
    std::string gaia_id_accessor;

    gaia_id_accessor.append("gaia::expressions::member_accessor_t");
    gaia_id_accessor.append("<");
    gaia_id_accessor.append(code.GetValue("CLASS_NAME"));
    gaia_id_accessor.append(", ");
    gaia_id_accessor.append("gaia::common::gaia_id_t");
    gaia_id_accessor.append(">");
    expr_variable = field_facade_t::generate_expr_variable(
        code.GetValue("CLASS_NAME"),
        gaia_id_accessor,
        "gaia_id");
    code += expr_variable.first;

    for (const auto& field : m_table.fields())
    {
        expr_variable = field.generate_expr_variable();
        code += expr_variable.first;
    }

    for (auto& link : m_table.incoming_links())
    {
        expr_variable = field_facade_t::generate_expr_variable(m_table.class_name(), link.expression_accessor(), link.field_name());
        code += expr_variable.first;
    }

    for (auto& link : m_table.outgoing_links())
    {
        expr_variable = field_facade_t::generate_expr_variable(m_table.class_name(), link.expression_accessor(), link.field_name());
        code += expr_variable.first;
    }

    code.DecrementIdentLevel();
    code += "};";
    code += "using expr = expr_<void>;";
    return code.ToString();
}

std::string class_writer_t::generate_private_constructor()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "explicit {{CLASS_NAME}}(gaia::common::gaia_id_t id) : dac_object_t(id) {}";
    return code.ToString();
}

std::string class_writer_t::generate_friend_declarations()
{
    flatbuffers::CodeWriter code = create_code_writer();
    code += "friend class dac_object_t<c_gaia_type_{{TABLE_NAME}}, {{CLASS_NAME}}, internal::{{TABLE_NAME}}, "
            "internal::{{TABLE_NAME}}T>;";
    if (m_table.needs_reference_class())
    {
        code += "friend class {{TABLE_NAME}}_ref_t;";
    }
    return code.ToString();
}

std::string class_writer_t::generate_close_class_definition()
{
    return "};\n";
}

void class_writer_t::increment_indent()
{
    m_indent_level++;
}

void class_writer_t::decrement_indent()
{
    ASSERT_PRECONDITION(m_indent_level > 0, "Indent level cannot be negative.");
    m_indent_level--;
}

std::string class_writer_t::generate_method_params(std::vector<field_facade_t> fields)
{
    std::string param_list;

    bool first = true;
    for (const auto& field : fields)
    {
        if (!first)
        {
            param_list += ", ";
        }
        else
        {
            first = false;
        }
        bool is_function_parameter = true;
        param_list += field.field_type(is_function_parameter) + " ";
        param_list += field.field_name();
    }
    return param_list;
}

std::string class_writer_t::generate_expr_namespace()
{
    flatbuffers::CodeWriter code = create_code_writer();

    code += "namespace {{TABLE_NAME}}_expr {";
    code.IncrementIdentLevel();
    code += "static auto& gaia_id = {{CLASS_NAME}}::expr::gaia_id;";

    for (const field_facade_t& field : m_table.fields())
    {
        code.SetValue("FIELD_NAME", field.field_name());
        code += "static auto& {{FIELD_NAME}} = {{CLASS_NAME}}::expr::{{FIELD_NAME}};";
    }

    for (auto& link : m_table.incoming_links())
    {
        code.SetValue("FIELD_NAME", link.field_name());
        code += "static auto& {{FIELD_NAME}} = {{CLASS_NAME}}::expr::{{FIELD_NAME}};";
    }

    for (auto& link : m_table.outgoing_links())
    {
        code.SetValue("FIELD_NAME", link.field_name());
        code += "static auto& {{FIELD_NAME}} = {{CLASS_NAME}}::expr::{{FIELD_NAME}};";
    }
    code.DecrementIdentLevel();
    code += "} // {{TABLE_NAME}}_expr";

    return code.ToString();
}

std::string class_writer_t::generate_expr_instantiation_cpp()
{
    flatbuffers::CodeWriter code = create_code_writer();

    // Initialization of static DAC expressions. For C++11 compliance we are not using
    // inline variables which are available in C++17.
    std::pair<std::string, std::string> expr_variable;

    std::string gaia_id_accessor;

    gaia_id_accessor.append("gaia::expressions::member_accessor_t");
    gaia_id_accessor.append("<");
    gaia_id_accessor.append(m_table.class_name());
    gaia_id_accessor.append(", ");
    gaia_id_accessor.append("gaia::common::gaia_id_t");
    gaia_id_accessor.append(">");

    expr_variable
        = field_facade_t::generate_expr_variable(
            m_table.class_name(),
            gaia_id_accessor,
            "gaia_id");
    code += expr_variable.second;

    for (const auto& field : m_table.fields())
    {
        expr_variable = field.generate_expr_variable();
        code += expr_variable.second;
    }

    for (auto& link : m_table.incoming_links())
    {
        expr_variable = field_facade_t::generate_expr_variable(m_table.class_name(), link.expression_accessor(), link.field_name());
        code += expr_variable.second;
    }

    for (auto& link : m_table.outgoing_links())
    {
        expr_variable = field_facade_t::generate_expr_variable(m_table.class_name(), link.expression_accessor(), link.field_name());
        code += expr_variable.second;
    }

    return code.ToString();
}

std::string class_writer_t::generate_ref_class()
{
    if (!m_table.needs_reference_class())
    {
        return "\\";
    }

    flatbuffers::CodeWriter code = create_code_writer();

    code += "class {{TABLE_NAME}}_ref_t : public {{CLASS_NAME}}, direct_access::dac_base_reference_t {";
    code += "public:";
    code.IncrementIdentLevel();
    code += "{{TABLE_NAME}}_ref_t() = delete;";
    code += "{{TABLE_NAME}}_ref_t(gaia::common::gaia_id_t parent, gaia::common::gaia_id_t child, "
            "gaia::common::reference_offset_t child_offset);";
    code += "bool disconnect();";
    code += "bool connect(gaia::common::gaia_id_t id);";
    code += "bool connect(const {{CLASS_NAME}}& object);";
    code.DecrementIdentLevel();
    code += "};";
    return code.ToString();
}

std::string class_writer_t::generate_ref_class_cpp()
{
    if (!m_table.needs_reference_class())
    {
        return "\\";
    }

    flatbuffers::CodeWriter code = create_code_writer();

    // Constructor.
    code += "{{TABLE_NAME}}_ref_t::{{TABLE_NAME}}_ref_t(gaia::common::gaia_id_t parent, "
            "gaia::common::gaia_id_t child, gaia::common::reference_offset_t child_offset)";
    code.IncrementIdentLevel();
    code += ": {{CLASS_NAME}}(child), direct_access::dac_base_reference_t(parent, child_offset)";
    code.DecrementIdentLevel();
    code += "{";
    code += "}";
    code += "";

    // disconnect()
    code += "bool {{TABLE_NAME}}_ref_t::disconnect()";
    code += "{";
    code.IncrementIdentLevel();
    code += "if (dac_base_reference_t::disconnect(this->gaia_id()))";
    code += "{";
    code.IncrementIdentLevel();
    code += "this->set(gaia::common::c_invalid_gaia_id);";
    code += "return true;";
    code.DecrementIdentLevel();
    code += "}";
    code += "return false;";
    code.DecrementIdentLevel();
    code += "}";
    code += "";

    // connect(gaia_id_t)
    code += "bool {{TABLE_NAME}}_ref_t::connect(gaia::common::gaia_id_t id)";
    code += "{";
    code.IncrementIdentLevel();
    code += "if (dac_base_reference_t::connect(this->gaia_id(), id))";
    code += "{";
    code.IncrementIdentLevel();
    code += "this->set(id);";
    code += "return true;";
    code.DecrementIdentLevel();
    code += "}";
    code += "return false;";
    code.DecrementIdentLevel();
    code += "}";
    code += "";

    // connect(dac_class_ref_t)
    code += "bool {{TABLE_NAME}}_ref_t::connect(const {{CLASS_NAME}}& object)";
    code += "{";
    code.IncrementIdentLevel();
    code += "return connect(object.gaia_id());";
    code.DecrementIdentLevel();
    code += "}";
    code += "";

    return code.ToString();
}

} // namespace generate
} // namespace catalog
} // namespace gaia

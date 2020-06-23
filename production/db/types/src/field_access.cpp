/////////////////////////////////////////////
// Copyright (c) Gaia Platform LLC
// All rights reserved.
/////////////////////////////////////////////

#include <field_access.hpp>

#include <sstream>

using namespace std;
using namespace gaia::db::types;

invalid_schema::invalid_schema()
{
    m_message = "Invalid binary flatbuffers schema.";
}

missing_root_type::missing_root_type()
{
    m_message = "Root type is missing in binary flatbuffers schema.";
}

invalid_serialized_data::invalid_serialized_data()
{
    m_message = "Cannot deserialize data.";
}

invalid_field_position::invalid_field_position(uint16_t position)
{
    stringstream string_stream;
    string_stream << "No field could be found for position: " << position << ".";
    m_message = string_stream.str();
}

unhandled_field_type::unhandled_field_type(size_t field_type)
{
    stringstream string_stream;
    string_stream << "Cannot handle field type: " << field_type << ".";
    m_message = string_stream.str();
}

invalid_serialized_field_data::invalid_serialized_field_data(uint16_t position)
{
    stringstream string_stream;
    string_stream << "Cannot deserialize data for field position: " << position << ".";
    m_message = string_stream.str();
}

void gaia::db::types::initialize_field_cache_from_binary_schema(
    field_cache_t* field_cache,
    uint8_t* binary_schema)
{
    // Deserialize the schema.
    const reflection::Schema* schema = reflection::GetSchema(binary_schema);
    if (schema == nullptr)
    {
        throw invalid_schema();
    }

    // Get the type of the schema's root object.
    const reflection::Object* root_type = schema->root_table();
    if (root_type == nullptr)
    {
        throw missing_root_type();
    }

    // Get the collection of fields
    // and insert each element under its corresponding field id.
    auto fields = root_type->fields();
    for (size_t i = 0; i < fields->Length(); i++)
    {
        const reflection::Field* current_field = fields->Get(i);
        field_cache->set_field(current_field->id(), current_field);
    }
}

data_holder_t gaia::db::types::get_table_field_value(
    uint64_t type_id,
    uint8_t* serialized_data,
    uint8_t* binary_schema,
    uint16_t field_position)
{
    data_holder_t result;

    // First, we parse the serialized data to get its root object.
    const flatbuffers::Table* root_table = flatbuffers::GetAnyRoot(serialized_data);
    if (root_table == nullptr)
    {
        throw invalid_serialized_data();
    }

    // Get hold of the type cache and lookup the field cache for our type.
    type_cache_t* type_cache = type_cache_t::get_type_cache();
    auto_field_cache_t auto_field_cache;
    type_cache->get_field_cache(type_id, auto_field_cache);
    const field_cache_t* field_cache = auto_field_cache.get();

    // If data is not available for our type, we load it locally from the binary schema provided to us.
    field_cache_t local_field_cache;
    if (field_cache == nullptr)
    {
        initialize_field_cache_from_binary_schema(&local_field_cache, binary_schema);
        field_cache = &local_field_cache;
    }

    // Lookup field information in the field cache.
    const reflection::Field* field = field_cache->get_field(field_position);
    if (field == nullptr)
    {
        throw invalid_field_position(field_position);
    }

    // Read field value according to its type.
    result.type = field->type()->base_type();
    if (field->type()->base_type() == reflection::String)
    {
        const flatbuffers::String* field_value = flatbuffers::GetFieldS(*root_table, *field);
        if (field_value == nullptr)
        {
            throw invalid_serialized_data();
        }

        result.hold.string_value = field_value->c_str();
    }
    else if (flatbuffers::IsInteger(field->type()->base_type()))
    {
        result.hold.integer_value = flatbuffers::GetAnyFieldI(*root_table, *field);
    }
    else if (flatbuffers::IsFloat(field->type()->base_type()))
    {
        result.hold.float_value = flatbuffers::GetAnyFieldF(*root_table, *field);
    }
    else
    {
        throw unhandled_field_type(field->type()->base_type());
    }

    return result;
}
/*
 * Copyright (C) 2019 ScyllaDB
 */

#pragma once

#include <string>

// Get the program build_id.
//
// May throw if encountered an error while parsing the dl_phdr.
std::string get_build_id();

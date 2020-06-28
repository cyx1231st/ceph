add_library(seastore-onode STATIC
  tree.cc)

add_executable(test_onode_tree
  main.cc)

target_link_libraries(test_onode_tree
  seastore-onode global fmt::fmt)

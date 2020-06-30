add_library(seastore-onode STATIC
  node.cc
  node_impl.cc
  stages/item_iterator_stage.cc
  stages/key_layout.cc
  stages/node_layout.cc
  stages/node_stage.cc
  stages/sub_items_stage.cc
  tree.cc)

add_executable(test_onode_tree
  main.cc)

target_link_libraries(test_onode_tree
  seastore-onode global fmt::fmt)

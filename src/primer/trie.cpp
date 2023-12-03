#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  auto k_item = key.begin();
  auto k_last = key.end();
  auto curr = root_;
  while (k_item != k_last && curr != nullptr) {
    if (curr->children_.count(*k_item) == 1) {
      curr = curr->children_.at(*k_item);
    } else {
      curr = nullptr;
    }

    k_item++;
  }

  if (curr == nullptr) {
    return nullptr;
  }

  if (!curr->is_value_node_) {
    return nullptr;
  }

  const TrieNodeWithValue<T>* nv = dynamic_cast<const TrieNodeWithValue<T>*>(curr.get());
  if (nv == nullptr) {
    return nullptr;
  }

  if (nv->value_ == nullptr) {
    return nullptr;
  }

  return nv->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  if (key.size() == 0) {
    // case: empty trie, value added to the root node
    auto new_root = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    if (root_ != nullptr) {
      // case: non-empty trie, value added to the root node
      new_root->children_ = root_->children_;
    }

    return Trie(new_root);
  }

  std::shared_ptr<TrieNode> new_root;
  if (root_ != nullptr) {
    new_root = std::shared_ptr<TrieNode>(std::move(root_->Clone()));
  } else {
    new_root = std::make_shared<TrieNode>();
  }

  auto k_item = key.begin();
  auto k_last = key.end() - 1;
  std::shared_ptr<TrieNode> curr = new_root;
  std::shared_ptr<TrieNode> node;
  while (k_item != k_last) {
    if (curr->children_.count(*k_item) == 1) {
        node = std::shared_ptr<TrieNode>(std::move(curr->children_.at(*k_item)->Clone()));
    } else {
        node = std::make_shared<TrieNode>();
    }

    curr->children_[*k_item] = node;
    curr = node;
    k_item++;
  }

  auto nv = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  if (curr->children_.count(*k_item) == 1) {
    // case: non-empty trie, value replacing an existing leaf node
    std::shared_ptr<const TrieNode> n = curr->children_.at(*k_item);
    if (!n->children_.empty()) {
      // case: non-empty trie, value replacing a non-leaf node
      nv->children_ = n->children_;
    }
    curr->children_[*k_item] = nv;
  } else {
    // case: non-empty trie, value added to a leaf node
    curr->children_[*k_item] = nv;
  }

  return Trie(new_root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (root_ == nullptr) {
    return *this;
  }

  std::vector<std::shared_ptr<const TrieNode>> path;
  path.push_back(root_);

  auto k_item = key.begin();
  auto k_last = key.end();
  auto curr = root_;
  while (k_item != k_last) {
    if (curr->children_.count(*k_item) == 0) {
      // case: key not found
      return *this;
    }

    curr = curr->children_.at(*k_item);
    path.push_back(curr);
    k_item++;
  }

  auto rk_item = key.rbegin();
  std::shared_ptr<TrieNode> node = nullptr;
  if (path.back()->children_.empty()) {
    // case: removing a leaf
    path.pop_back();
  } else if (path.back()->is_value_node_) {
    // case: removing a non-leaf node which has a value and some children
    node = std::make_shared<TrieNode>(path.back()->children_);
    path.pop_back();
  } else {
    // case: removing a non-leaf node which doesn't have a value, but has some children
    return *this;
  }

  if (node == nullptr) {
    while (!path.empty() && path.back()->children_.size() < 2 && !path.back()->is_value_node_) {
      // case: ancestor of the removed node without other children and value
      path.pop_back();
      rk_item++;
    }
  }

  std::shared_ptr<TrieNode> n;
  while (!path.empty()) {
    n = std::shared_ptr<TrieNode>(std::move(path.back()->Clone()));
    if (node != nullptr) {
      n->children_[*rk_item] = node;
    } else {
      n->children_.erase(*rk_item);
    }

    node = n;
    path.pop_back();
    rk_item++;
  }

  return Trie(node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub

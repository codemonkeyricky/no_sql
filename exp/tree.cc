
#include <algorithm>
#include <cmath>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <vector>

struct TreeNodeInline {
    double Value;
    int LeftChild = -1;
    int RightChild = -1;
};

struct TreeNode {
    double Value;
    TreeNode* LeftChild = nullptr;
    TreeNode* RightChild = nullptr;
    int Height = 0;
};

double findRoot(std::vector<double>& values) {
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

std::vector<int> leafIndexes(std::vector<TreeNodeInline>& treeArray,
                             int rootIndex) {
    const auto& node = treeArray[rootIndex];
    if (node.LeftChild == -2 && node.RightChild == -1) {
        return {rootIndex};
    }

    std::vector<int> leftIndexes = leafIndexes(treeArray, node.LeftChild);
    std::vector<int> rightIndexes = leafIndexes(treeArray, node.RightChild);
    leftIndexes.insert(leftIndexes.end(), rightIndexes.begin(),
                       rightIndexes.end());
    return leftIndexes;
};

TreeNode* createTree(const std::vector<double>& values) {
    if (values.empty()) {
        return nullptr;
    }

    std::vector<double> sortedValues = values;
    double value = findRoot(sortedValues);

    std::vector<double> leftValues, rightValues;
    for (double v : sortedValues) {
        if (v < value) {
            leftValues.push_back(v);
        } else if (v > value) {
            rightValues.push_back(v);
        }
    }

    TreeNode* leftSubtree = createTree(leftValues);
    TreeNode* rightSubtree = createTree(rightValues);

    int height = 0;
    if (leftSubtree) {
        height = std::max(height, leftSubtree->Height);
    }
    if (rightSubtree) {
        height = std::max(height, rightSubtree->Height);
    }

    return new TreeNode{value, leftSubtree, rightSubtree, height + 1};
}

int max(int a, int b) { return (a > b) ? a : b; }

std::vector<TreeNodeInline>
createTreePreorder(const std::vector<double>& values) {
    TreeNode* tree = createTree(values);
    std::vector<TreeNodeInline> treeArray(values.size());
    int nextOpenSlot = 0;

    std::function<int(TreeNode*)> fillTreeArray = [&](TreeNode* node) -> int {
        if (!node)
            return nextOpenSlot;
        int currentSlot = nextOpenSlot++;
        treeArray[currentSlot] = TreeNodeInline{node->Value, -1, -1};
        if (node->LeftChild) {
            treeArray[currentSlot].LeftChild = nextOpenSlot;
            fillTreeArray(node->LeftChild);
        }
        if (node->RightChild) {
            treeArray[currentSlot].RightChild = nextOpenSlot;
            fillTreeArray(node->RightChild);
        }
        return nextOpenSlot;
    };

    fillTreeArray(tree);
    return treeArray;
}

std::vector<TreeNodeInline>
createTreeLeveled(const std::vector<double>& values) {
    TreeNode* tree = createTree(values);
    if (!tree)
        return {};

    std::vector<TreeNodeInline> treeArray(values.size());
    int currentPos = 0;

    for (int level = 0; level < tree->Height; ++level) {
        std::function<std::vector<TreeNode*>(TreeNode*, int)> getLevel =
            [&](TreeNode* node, int currentLevel) -> std::vector<TreeNode*> {
            if (!node)
                return {};
            if (currentLevel == 0)
                return {node};
            auto left = getLevel(node->LeftChild, currentLevel - 1);
            auto right = getLevel(node->RightChild, currentLevel - 1);
            left.insert(left.end(), right.begin(), right.end());
            return left;
        };

        auto nodesAtLevel = getLevel(tree, level);
        if (nodesAtLevel.size() != (1u << level)) {
            throw std::runtime_error("Unexpected nodes at level");
        }

        int nextLevelStart = currentPos + nodesAtLevel.size();
        for (size_t upperLevelNodePosition = 0;
             upperLevelNodePosition < nodesAtLevel.size();
             ++upperLevelNodePosition) {
            int lChild = -1, rChild = -1;
            if (tree->Height - 1 > level) {
                lChild = nextLevelStart + upperLevelNodePosition * 2;
                rChild = nextLevelStart + upperLevelNodePosition * 2 + 1;
            }
            treeArray[currentPos++] = TreeNodeInline{
                nodesAtLevel[upperLevelNodePosition]->Value, lChild, rChild};
        }
    }
    return treeArray;
}

std::vector<TreeNodeInline>
createTreeCacheOblivious(const std::vector<double>& values) {
    TreeNode* tree = createTree(values);
    std::vector<TreeNodeInline> treeArray(values.size());

    auto splitFunc = [](TreeNode* t) -> int { return t->Height / 2; };

    std::function<int(std::vector<TreeNodeInline>&, int, TreeNode*,
                      std::function<int(TreeNode*)>)>
        fillTreeArraySplitting =
            [&](std::vector<TreeNodeInline>& treeArray, int nextOpenSpot,
                TreeNode* tree,
                std::function<int(TreeNode*)> splitFunc) -> int {
        if (!tree) {
            throw std::runtime_error("Tree can't be null");
        }

        if (tree->Height == 1) {
            treeArray[nextOpenSpot] = TreeNodeInline{tree->Value, -1, -1};
            return nextOpenSpot + 1;
        }

        auto splitTree = [&](TreeNode* tree, int splitHeight)
            -> std::pair<TreeNode*, std::vector<TreeNode*>> {
            if (!tree)
                throw std::runtime_error("Tree can't be null");

            std::function<std::pair<TreeNode*, std::vector<TreeNode*>>(
                TreeNode*, int)>
                splitTreeBrokenHeight = [&](TreeNode* tree, int height)
                -> std::pair<TreeNode*, std::vector<TreeNode*>> {
                if (!tree)
                    throw std::runtime_error("Tree can't be null");

                if (height == 1) {
                    std::vector<TreeNode*> leaves;
                    if (tree->LeftChild)
                        leaves.push_back(tree->LeftChild);
                    if (tree->RightChild)
                        leaves.push_back(tree->RightChild);
                    return {new TreeNode{tree->Value, nullptr, nullptr, -1},
                            leaves};
                }

                auto [leftChild, lLeaves] =
                    splitTreeBrokenHeight(tree->LeftChild, height - 1);
                auto [rightChild, rLeaves] =
                    splitTreeBrokenHeight(tree->RightChild, height - 1);

                TreeNode* root =
                    new TreeNode{tree->Value, leftChild, rightChild, -1};
                std::vector<TreeNode*> combinedLeaves = lLeaves;
                combinedLeaves.insert(combinedLeaves.end(), rLeaves.begin(),
                                      rLeaves.end());
                return {root, combinedLeaves};
            };

            auto [root, leaves] = splitTreeBrokenHeight(tree, splitHeight);
            return {root, leaves};
        };

        auto [top, leaves] = splitTree(tree, splitFunc(tree));
        int firstLeafSpot =
            fillTreeArraySplitting(treeArray, nextOpenSpot, top, splitFunc);

        std::vector<int> leafLocations;
        int nextLeafSpot = firstLeafSpot;
        for (TreeNode* leaf : leaves) {
            leafLocations.push_back(nextLeafSpot);
            nextLeafSpot = fillTreeArraySplitting(treeArray, nextLeafSpot, leaf,
                                                  splitFunc);
        }

        std::vector<int> unboundLeaves = leafIndexes(treeArray, nextOpenSpot);

        if (leafLocations.size() != unboundLeaves.size() * 2) {
            throw std::runtime_error("Number of leaves is wrong");
        }

        size_t nextLeaf = 0;
        for (int leafIndex : unboundLeaves) {
            treeArray[leafIndex].LeftChild = leafLocations[nextLeaf++];
            treeArray[leafIndex].RightChild = leafLocations[nextLeaf++];
        }

        return nextLeafSpot;
    };

    fillTreeArraySplitting(treeArray, 0, tree, splitFunc);
    return treeArray;
}

int main() {
    std::vector<double> values = {3.0, 1.0, 4.0, 1.5, 2.0};
    auto treeArray = createTreeCacheOblivious(values);

    for (const auto& node : treeArray) {
        std::cout << "Value: " << node.Value
                  << ", LeftChild: " << node.LeftChild
                  << ", RightChild: " << node.RightChild << '\n';
    }

    return 0;
}

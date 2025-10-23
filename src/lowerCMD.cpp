#include "lowerCMD.h"

std::string lowercase_command(std::string cmd){
  std::string lowerized;
  for (char c : cmd) lowerized += std::tolower(c);
  return lowerized;
}
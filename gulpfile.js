const path = require('path')
const { task, src, dest } = require('gulp')

task('build:icons', copyIcons)

function copyIcons () {
  const iconSources = [
    path.resolve('nodes', '**', '*.{png,svg}'),
    path.resolve('credentials', '**', '*.{png,svg}')
  ]
  const outputDir = path.resolve('dist')

  return src(iconSources, { base: '.' }).pipe(dest(outputDir))
}

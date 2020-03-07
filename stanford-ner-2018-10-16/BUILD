jar_library(
  name='stanford-ner',
  jars=[
    jar(org='edu.stanford', name='nlp', rev='???',
        url='file:///Users/dmcclanahan/projects/active/sars-cov-2-coverage/stanford-ner-2018-10-16/stanford-ner-3.9.2.jar'),
  ],
)

scala_library(
  name='ner-lib',
  dependencies=[
    ':stanford-ner',
  ],
)

jvm_binary(
  name='ner-piped',
  dependencies=[
    ':stanford-ner',
    ':ner-lib',
  ],
  main='NERPiped',
  extra_jvm_options=['-Xmx4g'],
)

import * as THREE from '../vendor/three.module.min.js';

const readAccent = () => {
  const raw = getComputedStyle(document.documentElement).getPropertyValue('--accent').trim();
  const value = raw.startsWith('#') ? raw.slice(1) : '';
  if (value.length === 6) return '#' + value;
  if (value.length === 3) return '#' + value.split('').map((c) => c + c).join('');
  return '#4f46e5';
};

const mix = (hex, target, amount) => {
  const color = new THREE.Color(hex);
  const other = new THREE.Color(target);
  return color.lerp(other, amount);
};

export function initHeroThree() {
  const canvas = document.getElementById('hero-canvas');
  const hero = document.querySelector('.hero');
  if (!canvas || !hero) return;
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

  let renderer;
  try {
    renderer = new THREE.WebGLRenderer({
      canvas,
      alpha: true,
      antialias: true,
      powerPreference: 'low-power'
    });
  } catch (error) {
    canvas.style.display = 'none';
    return;
  }

  renderer.setClearColor(0x000000, 0);
  renderer.outputColorSpace = THREE.SRGBColorSpace;

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(50, 1, 0.1, 100);
  camera.position.set(0, 0, 9.4);

  const isSmall = window.matchMedia('(max-width: 720px)').matches;
  const count = isSmall ? 38 : 64;
  const accent = readAccent();
  const palette = [new THREE.Color(accent), mix(accent, '#ffffff', 0.45), mix(accent, '#0f172a', 0.38)];
  const positions = new Float32Array(count * 3);
  const colors = new Float32Array(count * 3);
  const velocities = [];
  const spanX = 13;
  const spanY = 8.5;
  const spanZ = 6;

  for (let i = 0; i < count; i += 1) {
    const offset = i * 3;
    positions[offset] = (Math.random() - 0.5) * spanX;
    positions[offset + 1] = (Math.random() - 0.5) * spanY;
    positions[offset + 2] = (Math.random() - 0.5) * spanZ;
    velocities.push(new THREE.Vector3(
      (Math.random() - 0.5) * 0.0016,
      (Math.random() - 0.5) * 0.0011,
      (Math.random() - 0.5) * 0.0009
    ));
    const color = palette[i % palette.length];
    colors[offset] = color.r;
    colors[offset + 1] = color.g;
    colors[offset + 2] = color.b;
  }

  const pointGeometry = new THREE.BufferGeometry();
  pointGeometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
  pointGeometry.setAttribute('color', new THREE.BufferAttribute(colors, 3));

  const points = new THREE.Points(pointGeometry, new THREE.PointsMaterial({
    size: isSmall ? 0.075 : 0.09,
    vertexColors: true,
    transparent: true,
    opacity: 0.86,
    depthWrite: false,
    sizeAttenuation: true
  }));

  const maxPairs = (count * (count - 1)) / 2;
  const linePositions = new Float32Array(maxPairs * 6);
  const lineGeometry = new THREE.BufferGeometry();
  lineGeometry.setAttribute('position', new THREE.BufferAttribute(linePositions, 3));
  lineGeometry.setDrawRange(0, 0);

  const lines = new THREE.LineSegments(lineGeometry, new THREE.LineBasicMaterial({
    color: new THREE.Color(accent),
    transparent: true,
    opacity: 0.2,
    depthWrite: false
  }));

  const group = new THREE.Group();
  group.add(points, lines);
  scene.add(group);

  const pointer = { x: 0, y: 0 };
  hero.addEventListener('pointermove', (event) => {
    const rect = hero.getBoundingClientRect();
    pointer.x = (event.clientX - rect.left) / rect.width - 0.5;
    pointer.y = (event.clientY - rect.top) / rect.height - 0.5;
  }, { passive: true });

  const updateLines = () => {
    let cursor = 0;
    const position = pointGeometry.attributes.position.array;
    const threshold = isSmall ? 2 : 2.25;
    const thresholdSq = threshold * threshold;
    for (let i = 0; i < count; i += 1) {
      const a = i * 3;
      for (let j = i + 1; j < count; j += 1) {
        const b = j * 3;
        const dx = position[a] - position[b];
        const dy = position[a + 1] - position[b + 1];
        const dz = position[a + 2] - position[b + 2];
        if (dx * dx + dy * dy + dz * dz < thresholdSq) {
          linePositions[cursor] = position[a];
          linePositions[cursor + 1] = position[a + 1];
          linePositions[cursor + 2] = position[a + 2];
          linePositions[cursor + 3] = position[b];
          linePositions[cursor + 4] = position[b + 1];
          linePositions[cursor + 5] = position[b + 2];
          cursor += 6;
        }
      }
    }
    lineGeometry.setDrawRange(0, cursor / 3);
    lineGeometry.attributes.position.needsUpdate = true;
  };

  let width = 1;
  let height = 1;
  let visible = true;
  let frame = 0;
  let rotation = 0;
  const clock = new THREE.Clock();

  const resize = () => {
    const rect = hero.getBoundingClientRect();
    width = Math.max(1, Math.round(rect.width));
    height = Math.max(1, Math.round(rect.height));
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, isSmall ? 1.25 : 1.55));
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
  };

  const render = () => {
    const delta = Math.min(clock.getDelta(), 0.05);
    const position = pointGeometry.attributes.position.array;
    for (let i = 0; i < count; i += 1) {
      const offset = i * 3;
      const velocity = velocities[i];
      position[offset] += velocity.x * delta * 60;
      position[offset + 1] += velocity.y * delta * 60;
      position[offset + 2] += velocity.z * delta * 60;
      if (Math.abs(position[offset]) > spanX / 2) velocity.x *= -1;
      if (Math.abs(position[offset + 1]) > spanY / 2) velocity.y *= -1;
      if (Math.abs(position[offset + 2]) > spanZ / 2) velocity.z *= -1;
    }
    pointGeometry.attributes.position.needsUpdate = true;
    updateLines();
    rotation += 0.00065;
    group.rotation.y = rotation + pointer.x * 0.2;
    group.rotation.x = pointer.y * 0.12;
    renderer.render(scene, camera);
    if (!document.hidden && visible) {
      frame = window.requestAnimationFrame(render);
    } else {
      frame = 0;
    }
  };

  resize();
  updateLines();
  renderer.render(scene, camera);
  hero.classList.add('has-three');
  render();

  window.addEventListener('resize', () => {
    resize();
    if (!frame) {
      updateLines();
      renderer.render(scene, camera);
    }
  }, { passive: true });

  if ('IntersectionObserver' in window) {
    new IntersectionObserver((entries) => {
      visible = entries[0].isIntersecting;
      if (visible && !frame) {
        clock.getDelta();
        render();
      }
    }, { threshold: 0 }).observe(hero);
  }

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && visible && !frame) {
      clock.getDelta();
      render();
    }
  });

  canvas.addEventListener('webglcontextlost', (event) => {
    event.preventDefault();
    if (frame) window.cancelAnimationFrame(frame);
    frame = 0;
    hero.classList.remove('has-three');
  });
}
